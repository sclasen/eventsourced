package org.eligosource.eventsourced.journal

import DynamoDBJournal._
import akka.actor.ActorRef
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

  val log = context.system.log

  val counterAtt = S(props.eventsourcedApp + "COUNTER")

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
    log.debug("storedCounter")
    val res: GetItemResult = dynamo.getItem(new GetItemRequest().withTableName(props.journalTable).withKey(counterKey).withConsistentRead(true))
    Option(res.getItem).map(_.get(Event)).map(a => counterFromBytes(a.getB.array())).getOrElse(0L)
  }

  def executeBatchReplayInMsgs(cmds: Seq[ReplayInMsgs], p: (Message, ActorRef) => Unit) {
    log.debug("executeBatchReplayInMsgs")
    cmds.foreach(cmd => replayIn(cmd, cmd.processorId, p(_, cmd.target)))
    sender ! ReplayDone
  }

  def executeReplayInMsgs(cmd: ReplayInMsgs, p: (Message) => Unit) {
    log.debug("executeReplayInMsgs")
    replayIn(cmd, cmd.processorId, p)
    sender ! ReplayDone
  }

  def executeReplayOutMsgs(cmd: ReplayOutMsgs, p: (Message) => Unit) {
    log.debug("executeReplayOutMsgs")
    replayOut(cmd, p)
    //sender ! ReplayDone needed???
  }


  def executeWriteOutMsg(cmd: WriteOutMsg) {
    log.debug("executeWriteOutMsg")
    write(cmd, cmd.message.clearConfirmationSettings)
    write(counterKey, counter)
    if (cmd.ackSequenceNr != SkipAck)
      executeWriteAck(WriteAck(cmd.ackProcessorId, cmd.channelId, cmd.ackSequenceNr))
    else
    //write a -1 to acks so we can be assured of non-nulls on the batch get in replay
      executeWriteAck(WriteAck(cmd.ackProcessorId, -1, cmd.ackSequenceNr))
  }

  def executeWriteInMsg(cmd: WriteInMsg) {
    log.debug("executeWriteInMsg")
    write(cmd, cmd.message.clearConfirmationSettings)
    write(counterKey, counter)
  }


  def executeWriteAck(cmd: WriteAck) {
    log.debug("executeWriteAckMsg")
    val atts = new java.util.HashMap[String, AttributeValueUpdate]()
    atts.put(Acks, new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(NS(cmd.channelId)))
    val updates: UpdateItemRequest = new UpdateItemRequest().withTableName(props.journalTable).withKey(cmd).withAttributeUpdates(atts)
    dynamo.updateItem(updates)
  }


  def replayOut(q: QueryRequest, p: (Message) => Unit) {
    val (messages, from) = queryAll(q)
    messages.foreach(p)
    from.foreach {
      k =>
        val more = q.withExclusiveStartKey(q.getExclusiveStartKey.withRangeKeyElement(k.getRangeKeyElement))
        replayOut(more, p)
    }
  }

  def replayIn(q: QueryRequest, processorId: Int, p: (Message) => Unit) {
    val (messages, from) = queryAll(q)
    confirmingChannels(processorId, messages).foreach(p)
    from.foreach {
      k =>
        val more = q.withExclusiveStartKey(q.getExclusiveStartKey.withRangeKeyElement(k.getRangeKeyElement))
        replayIn(more, processorId, p)
    }
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

      val ka = new KeysAndAttributes().withAttributesToGet(Acks).withKeys(keys).withConsistentRead(true)

      val batch = new BatchGetItemRequest().withRequestItems(Collections.singletonMap(props.journalTable, ka))

      val response = dynamo.batchGetItem(batch).getResponses.get(props.journalTable)

      messages.zipWithIndex.map {
        case (message, index) =>
          if (response.getItems.size() > index && response.getItems.get(index) != null) {
            val acks: Buffer[Int] = response.getItems.get(index).get(Acks).getNS.asScala.map(_.toInt)
            message.copy(acks = acks.filter(_ != -1))
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

  def write(key: DynamoKey, message: Array[Byte]) {
    val atts = new java.util.HashMap[String, AttributeValueUpdate]()
    atts.put(Event, UB(message))
    val req = new UpdateItemRequest().withKey(key).withTableName(props.journalTable).withAttributeUpdates(atts)
    dynamo.updateItem(req)
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

  implicit def replayOutToQuery(re: ReplayOutMsgs): QueryRequest =
    new QueryRequest()
      .withTableName(props.journalTable)
      .withConsistentRead(true)
      .withHashKeyValue(outKey(re.channelId))
      .withExclusiveStartKey(re)
}

object DynamoDBJournal {

  val Id = "id"
  val Sequence = "sequence"
  val Event = "event"
  val Counter = "counter"
  val Acks = "acks"

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
