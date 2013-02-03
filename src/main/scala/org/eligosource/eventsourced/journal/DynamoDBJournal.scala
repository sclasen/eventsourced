package org.eligosource.eventsourced.journal

import DynamoDBJournal._
import akka.actor.ActorRef
import annotation.tailrec
import collection.JavaConverters._
import collection.mutable.Buffer
import com.amazonaws.services.dynamodb.AmazonDynamoDB
import com.amazonaws.services.dynamodb.model._
import com.amazonaws.services.dynamodb.model.{Key => DynamoKey}
import java.nio.ByteBuffer
import java.util.Collections
import org.eligosource.eventsourced.core.Journal._
import org.eligosource.eventsourced.core.{Serialization, Message, Journal}

/**
 * Current status:
 *
 * Needs more error resilience.
 *
 * Needs a strategy for storing Messages with size > 64k.
 *
 * Could batch up some writes.
 */
class DynamoDBJournal(props: DynamoDBJournalProps) extends Journal {

  val serialization = Serialization(context.system)

  implicit def msgToBytes(msg: Message): Array[Byte] = serialization.serializeMessage(msg)

  implicit def msgFromBytes(bytes: Array[Byte]): Message = serialization.deserializeMessage(bytes)

  val channelMarker = Array(1.toByte)

  val log = context.system.log

  val counterAtt = S(props.eventsourcedApp + Counter)

  val counterKey =
    new DynamoKey()
      .withHashKeyElement(counterAtt)
      .withRangeKeyElement(N(0L))

  implicit val dynamo: AmazonDynamoDB = props.dynamo

  def executeDeleteOutMsg(cmd: DeleteOutMsg) {
    log.debug("executeDeleteOutMsg")
    val del: DeleteItemRequest = new DeleteItemRequest().withTableName(props.journalTable).withKey(cmd)
    dynamo.deleteItem(del)
  }

  protected def storedCounter: Long = {
    log.info("storedCounter")
    val res: GetItemResult = dynamo.getItem(new GetItemRequest().withTableName(props.journalTable).withKey(counterKey).withConsistentRead(true))
    Option(res.getItem).map(_.get(Event)).map(a => counterFromBytes(a.getB.array())).getOrElse(0L)
  }

  def executeBatchReplayInMsgs(cmds: Seq[ReplayInMsgs], p: (Message, ActorRef) => Unit) {
    log.info("executeBatchReplayInMsgs")
    cmds.foreach(cmd => replayIn(cmd, cmd.processorId, p(_, cmd.target)))
    sender ! ReplayDone
  }

  def executeReplayInMsgs(cmd: ReplayInMsgs, p: (Message) => Unit) {
    log.info("executeReplayInMsgs")
    replayIn(cmd, cmd.processorId, p)
    sender ! ReplayDone
  }

  def executeReplayOutMsgs(cmd: ReplayOutMsgs, p: (Message) => Unit) {
    log.info("executeReplayOutMsgs")
    replayOut(cmd, p)
    //sender ! ReplayDone needed???
  }


  def executeWriteOutMsg(cmd: WriteOutMsg) {
    log.debug("executeWriteOutMsg")

    val ack = {
      if (cmd.ackSequenceNr != SkipAck)
        putAck(WriteAck(cmd.ackProcessorId, cmd.channelId, cmd.ackSequenceNr))
      else
      //write a -1 to acks so we can be assured of non-nulls on the batch get in replay
        putAck(WriteAck(cmd.ackProcessorId, -1, cmd.ackSequenceNr))
    }

    batchWrite(
      put(cmd, cmd.message.clearConfirmationSettings),
      put(counterKey, counter), //conditional write?,
      ack
    )
  }

  def executeWriteInMsg(cmd: WriteInMsg) {
    log.debug("executeWriteInMsg")
    batchWrite(
      put(cmd, cmd.message.clearConfirmationSettings),
      put(counterKey, counter)
    )
  }


  def executeWriteAck(cmd: WriteAck) {
    batchWrite(putAck(cmd))
  }

  @tailrec
  private def replayOut(q: QueryRequest, p: (Message) => Unit) {
    val (messages, from) = queryAll(q)
    messages.foreach(p)
    val moreOpt = from.map {
      k =>
        q.withExclusiveStartKey(q.getExclusiveStartKey.withRangeKeyElement(k.getRangeKeyElement))
    }
    if (moreOpt.isEmpty) {} else replayOut(moreOpt.get, p)
  }

  @tailrec
  private def replayIn(q: QueryRequest, processorId: Int, p: (Message) => Unit) {
    val (messages, from) = queryAll(q)
    log.info(messages.size.toString)
    confirmingChannels(processorId, messages).foreach(p)
    val moreOpt = from.map {
      k =>
        q.withExclusiveStartKey(q.getExclusiveStartKey.withRangeKeyElement(k.getRangeKeyElement))
    }
    if (moreOpt.isEmpty) {} else replayIn(moreOpt.get, processorId, p)
  }

  def confirmingChannels(processorId: Int, messages: Buffer[Message]): Buffer[Message] = {
    if (messages.isEmpty) messages
    else {
      val keys = messages.map {
        message =>
          new DynamoKey()
            .withHashKeyElement(ackKey(processorId))
            .withRangeKeyElement(N(message.sequenceNr))
      }.asJava

      val ka = new KeysAndAttributes().withKeys(keys).withConsistentRead(true)

      val batch = new BatchGetItemRequest().withRequestItems(Collections.singletonMap(props.journalTable, ka))

      val response = dynamo.batchGetItem(batch).getResponses.get(props.journalTable)

      messages.zipWithIndex.map {
        case (message, index) =>
          if (response.getItems.size() > index && response.getItems.get(index) != null) {
            val chAcks = response.getItems.get(index).keySet().asScala.filter(!DynamoKeys.contains(_)).map(_.toInt).toSeq
            message.copy(acks = chAcks.filter(_ != -1))
          } else {
            message
          }
      }
    }
  }

  def queryAll(q: QueryRequest): (Buffer[Message], Option[DynamoKey]) = {
    val res: QueryResult = dynamo.query(q)
    val messages = res.getItems.asScala.map(m => m.get(Event).getB).map(b => msgFromBytes(b.array()))
    (messages, Option(res.getLastEvaluatedKey))
  }

  def put(key: DynamoKey, message: Array[Byte]): PutRequest = {
    val item = new java.util.HashMap[String, AttributeValue]
    item.put(Id, key.getHashKeyElement)
    item.put(Sequence, key.getRangeKeyElement)
    item.put(Event, B(message))
    new PutRequest().withItem(item)
  }

  def putAck(ack: WriteAck): PutRequest = {
    val item = new java.util.HashMap[String, AttributeValue]
    item.put(Id, ack.getHashKeyElement)
    item.put(Sequence, ack.getRangeKeyElement)
    item.put(ack.channelId.toString, B(channelMarker))
    new PutRequest().withItem(item)
  }


  def batchWrite(puts: PutRequest*) {
    /*val write = new java.util.HashMap[String, java.util.List[WriteRequest]]
    val writes = puts.map(new WriteRequest().withPutRequest(_)).asJava
    write.put(props.journalTable, writes)
    val batch = new BatchWriteItemRequest().withRequestItems(write)
    val res: BatchWriteItemResult = dynamo.batchWriteItem(batch)
    res.getUnprocessedItems.asScala.foreach {
      case (t, u) =>
        log.error("UNPROCESSED!")
        u.asScala.foreach {
          w => log.error(w.toString)
        }
    }*/
    puts.foreach {
      p => dynamo.putItem(new PutItemRequest().withTableName(props.journalTable).withItem(p.getItem))
    }

  }


  def S(value: String): AttributeValue = new AttributeValue().withS(value)

  def N(value: Long): AttributeValue = new AttributeValue().withN(value.toString)

  def NS(value: Long): AttributeValue = new AttributeValue().withNS(value.toString)

  def B(value: Array[Byte]): AttributeValue = new AttributeValue().withB(ByteBuffer.wrap(value))

  def UB(value: Array[Byte]): AttributeValueUpdate = new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(B(value))

  def inKey(procesorId: Int) = S(props.eventsourcedApp + "IN" + procesorId)

  def outKey(channelId: Int) = S(props.eventsourcedApp + "OUT" + channelId)

  def ackKey(processorId: Int) = S(props.eventsourcedApp + "ACK" + processorId)


  implicit def inToDynamoKey(cmd: WriteInMsg): DynamoKey =
    new DynamoKey()
      .withHashKeyElement(inKey(cmd.processorId))
      .withRangeKeyElement(N(cmd.message.sequenceNr))

  implicit def outToDynamoKey(cmd: WriteOutMsg): DynamoKey =
    new DynamoKey()
      .withHashKeyElement(outKey(cmd.channelId))
      .withRangeKeyElement(N(cmd.message.sequenceNr))

  implicit def delToDynamoKey(cmd: DeleteOutMsg): DynamoKey =
    new DynamoKey().
      withHashKeyElement(outKey(cmd.channelId))
      .withRangeKeyElement(N(cmd.msgSequenceNr))

  implicit def ackToDynamoKey(cmd: WriteAck): DynamoKey =
    new DynamoKey()
      .withHashKeyElement(ackKey(cmd.processorId))
      .withRangeKeyElement(N(cmd.ackSequenceNr))

  implicit def replayInToDynamoKey(cmd: ReplayInMsgs): DynamoKey =
    new DynamoKey().withHashKeyElement(inKey(cmd.processorId)).withRangeKeyElement(N(cmd.fromSequenceNr - 1))

  implicit def replayOutToDynamoKey(cmd: ReplayOutMsgs): DynamoKey =
    new DynamoKey().withHashKeyElement(outKey(cmd.channelId)).withRangeKeyElement(N(cmd.fromSequenceNr - 1))


  implicit def replayInToQuery(re: ReplayInMsgs): QueryRequest =
    new QueryRequest()
      .withTableName(props.journalTable)
      .withConsistentRead(true)
      .withHashKeyValue(inKey(re.processorId))
      .withExclusiveStartKey(re)
      .withLimit(100)

  implicit def replayOutToQuery(re: ReplayOutMsgs): QueryRequest =
    new QueryRequest()
      .withTableName(props.journalTable)
      .withConsistentRead(true)
      .withHashKeyValue(outKey(re.channelId))
      .withExclusiveStartKey(re)
      .withLimit(100)
}

object DynamoDBJournal {

  val Id = "id"
  val Sequence = "sequence"
  val Event = "event"
  val Counter = "COUNTER"
  val DynamoKeys = Set(Id, Sequence)

  val hashKey = new KeySchemaElement().withAttributeName("id").withAttributeType("S")
  val rangeKey = new KeySchemaElement().withAttributeName("sequence").withAttributeType("N")
  val schema = new KeySchema().withHashKeyElement(hashKey).withRangeKeyElement(rangeKey)


  def createJournal(table: String)(implicit dynamo: AmazonDynamoDB) {
    if (!dynamo.listTables(new ListTablesRequest()).getTableNames.contains(table)) {
      dynamo.createTable(new CreateTableRequest(table, schema).withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1).withWriteCapacityUnits(1)))
      waitForActiveTable(table)
    }
  }

  def waitForActiveTable(table: String, retries: Int = 100)(implicit dynamo: AmazonDynamoDB) {
    if (retries == 0) throw new RuntimeException("Timed out waiting for creation of:" + table)
    val desc = dynamo.describeTable(new DescribeTableRequest().withTableName(table))
    if (desc.getTable.getTableStatus != "ACTIVE") {
      Thread.sleep(1000)
      println("waiting to create table")
      waitForActiveTable(table, retries - 1)
    }
  }


}
