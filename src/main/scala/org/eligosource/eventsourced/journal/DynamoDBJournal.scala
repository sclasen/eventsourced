package org.eligosource.eventsourced.journal

import DynamoDBJournal._
import akka.actor.{Props, Actor, ActorRef}
import akka.pattern._
import akka.util._
import annotation.tailrec
import collection.JavaConverters._
import collection.mutable.Buffer
import com.amazonaws.services.dynamodb.AmazonDynamoDB
import com.amazonaws.services.dynamodb.model._
import com.amazonaws.services.dynamodb.model.{Key => DynamoKey}
import java.nio.ByteBuffer
import java.util.Collections
import org.eligosource.eventsourced.core.Channel.Deliver
import org.eligosource.eventsourced.core.Journal._
import org.eligosource.eventsourced.core.{Serialization, Message}
import scala.concurrent.duration._
import collection.immutable.IndexedSeq
import java.util

/**
 * Current status:
 *
 * Needs more error resilience.
 *
 * Needs a strategy for storing Messages with size > 64k.
 *
 * Could batch up some writes.
 */
class DynamoDBJournal(props: DynamoDBJournalProps) extends Actor {

  val serialization = Serialization(context.system)

  implicit def msgToBytes(msg: Message): Array[Byte] = serialization.serializeMessage(msg)

  implicit def msgFromBytes(bytes: Array[Byte]): Message = serialization.deserializeMessage(bytes)

  val channelMarker = Array(1.toByte)
  val countMarker = Array(1.toByte)

  val log = context.system.log

  def counterAtt(shard: Int) = S(props.eventsourcedApp + Counter + shard)

  def counterKey(shard: Int) =
    new DynamoKey()
      .withHashKeyElement(counterAtt(shard))
      .withRangeKeyElement(N(0L))

  implicit val dynamo: AmazonDynamoDB = props.dynamo

  implicit val ctx = context.system.dispatcher

  implicit val timeout = Timeout(50 seconds)


  protected def storedCounter: Long = {
    (1 to 100).map(counterKey).grouped(25).map {
      counterKeys =>
        val ka = new KeysAndAttributes().withKeys(counterKeys:_*).withAttributesToGet(Event).withConsistentRead(true)
        val tables = Collections.singletonMap(props.journalTable, ka)
        val res = dynamo.batchGetItem(new BatchGetItemRequest().withRequestItems(tables))
        val batch = res.getResponses.get(props.journalTable)
        val items: util.List[util.Map[String, AttributeValue]] = batch.getItems
        val counters: Buffer[Long] = items.asScala.map(_.get(Event)).map(a => counterFromBytes(a.getB.array()))
        counters.foldLeft(0L)(math.max(_,_))
    }.foldLeft(0L)(math.max(_,_))
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
    log.info(s"replayIn ${messages.size}")
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
    val write = new java.util.HashMap[String, java.util.List[WriteRequest]]
    val writes = puts.map(new WriteRequest().withPutRequest(_)).asJava
    write.put(props.journalTable, writes)
    val batch = new BatchWriteItemRequest().withRequestItems(write)
    val res: BatchWriteItemResult = dynamo.batchWriteItem(batch)
    res.getUnprocessedItems.asScala.foreach {
      case (t, u) =>
        log.error("UNPROCESSED!")
        u.asScala.foreach {
          w => log.error(w.toString)
          dynamo.putItem(new PutItemRequest().withTableName(props.journalTable).withItem(w.getPutRequest.getItem))
        }
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


  val deadLetters = context.system.deadLetters
  val resequencer: ActorRef = context.actorOf(Props(new Resequencer))

  val w: IndexedSeq[ActorRef] = (1 to props.asyncWriterCount).map {
    i => context.actorOf(Props(new Writer(i)))
  }

  val writers: Vector[ActorRef] = Vector(w:_*)

  private var _counter = 0L
  private var _counterResequencer = 1L

  def counter = _counter

  def counterResequencer = _counterResequencer

  def asyncWriteAndResequence(cmd: Any) {
    val ctr = counterResequencer
    val sdr = sender
    val idx = counterResequencer % props.asyncWriterCount
    val write = writers(idx.toInt) ?(_counterResequencer, cmd)
    write onSuccess {
      case _ => resequencer tell((ctr, cmd), sdr)
    }
    write onFailure {
      case _ => /* TODO: handle error */
    }
    _counterResequencer += 1L
  }

  def asyncResequence(cmd: Any) {
    resequencer forward(counterResequencer, cmd)
    _counterResequencer += 1L
  }

  def receive = {
    case cmd: WriteInMsg => {
      val c = if (cmd.genSequenceNr) cmd.withSequenceNr(counter)
      else {
        _counter = cmd.message.sequenceNr;
        cmd
      }
      asyncWriteAndResequence(c)
      _counter += 1L
    }
    case cmd: WriteOutMsg => {
      val c = if (cmd.genSequenceNr) cmd.withSequenceNr(counter)
      else {
        _counter = cmd.message.sequenceNr;
        cmd
      }
      asyncWriteAndResequence(c)
      _counter += 1L
    }
    case cmd: WriteAck => {
      asyncWriteAndResequence(cmd)
    }
    case cmd: DeleteOutMsg => {
      asyncWriteAndResequence(cmd)
    }
    case cmd: Loop => {
      asyncResequence(cmd)
    }
    case cmd: BatchReplayInMsgs => {
      asyncResequence(cmd)
    }
    case cmd: ReplayInMsgs => {
      asyncResequence(cmd)
    }
    case cmd: ReplayOutMsgs => {
      asyncResequence(cmd)
    }
    case cmd: BatchDeliverOutMsgs => {
      asyncResequence(cmd)
    }
    case cmd: SetCommandListener => {
      resequencer ! cmd
    }
  }

  override def preStart() {
    _counter = storedCounter + 1L
  }

  class Writer(counterShard: Int) extends Actor {
    def receive = {
      case (nr, cmd: WriteInMsg) => {
        executeWriteInMsg(cmd)
        sender !()
      }
      case (nr, cmd: WriteOutMsg) => {
        executeWriteOutMsg(cmd)
        sender !()
      }
      case (nr, cmd: WriteAck) => {
        executeWriteAck(cmd)
        sender !()
      }
      case (nr, cmd: DeleteOutMsg) => {
        executeDeleteOutMsg(cmd)
        sender !()
      }
    }

    def executeDeleteOutMsg(cmd: DeleteOutMsg) {
      val del: DeleteItemRequest = new DeleteItemRequest().withTableName(props.journalTable).withKey(cmd)
      dynamo.deleteItem(del)
    }

    def executeWriteOutMsg(cmd: WriteOutMsg) {
      val ack = {
        if (cmd.ackSequenceNr != SkipAck)
          putAck(WriteAck(cmd.ackProcessorId, cmd.channelId, cmd.ackSequenceNr))
        else
        //write a -1 to acks so we can be assured of non-nulls on the batch get in replay
          putAck(WriteAck(cmd.ackProcessorId, -1, cmd.ackSequenceNr))
      }

      batchWrite(
        put(cmd, cmd.message.clearConfirmationSettings),
        put(counterKey(counterShard), cmd.message.sequenceNr),
        ack
      )
    }

    def executeWriteInMsg(cmd: WriteInMsg) {
      batchWrite(
        put(cmd, cmd.message.clearConfirmationSettings),
        put(counterKey(counterShard), cmd.message.sequenceNr)
      )
    }

    def executeWriteAck(cmd: WriteAck) {
      batchWrite(putAck(cmd))
    }
  }


  class Resequencer extends Actor {

    import scala.collection.mutable.Map

    private val delayed = Map.empty[Long, (Any, ActorRef)]
    private var delivered = 0L
    private var commandListener: Option[ActorRef] = None

    def receive = {
      case (seqnr: Long, cmd) => resequence(seqnr, cmd, sender)
      case SetCommandListener(cl) => commandListener = cl
    }

    def execute(cmd: Any, sdr: ActorRef) = cmd match {
      case c: WriteInMsg => {
        c.target tell(Written(c.message), sdr)
        commandListener.foreach(_ ! cmd)
      }
      case c: WriteOutMsg => {
        c.target tell(Written(c.message), sdr)
        commandListener.foreach(_ ! cmd)
      }
      case c: WriteAck => {
        commandListener.foreach(_ ! cmd)
      }
      case c: DeleteOutMsg => {
        commandListener.foreach(_ ! cmd)
      }
      case Loop(msg, target) => {
        target forward (Looped(msg))
      }
      case BatchReplayInMsgs(replays) => {
        executeBatchReplayInMsgs(replays, (msg, target) => target tell(Written(msg), deadLetters), sdr)
      }
      case cmd: ReplayInMsgs => {
        executeReplayInMsgs(cmd, msg => cmd.target tell(Written(msg), deadLetters), sdr)
      }
      case cmd: ReplayOutMsgs => {
        executeReplayOutMsgs(cmd, msg => cmd.target tell(Written(msg), deadLetters), sdr)
      }
      case BatchDeliverOutMsgs(channels) => {
        channels.foreach(_ ! Deliver)
        sender ! DeliveryDone
      }
    }

    def executeBatchReplayInMsgs(cmds: Seq[ReplayInMsgs], p: (Message, ActorRef) => Unit, sender: ActorRef) {
      cmds.foreach(cmd => replayIn(cmd, cmd.processorId, p(_, cmd.target)))
      sender ! ReplayDone
    }

    def executeReplayInMsgs(cmd: ReplayInMsgs, p: (Message) => Unit, sender: ActorRef) {
      replayIn(cmd, cmd.processorId, p)
      sender ! ReplayDone
    }

    def executeReplayOutMsgs(cmd: ReplayOutMsgs, p: (Message) => Unit, sender: ActorRef) {
      replayOut(cmd, p)
      //sender ! ReplayDone needed???
    }

    @scala.annotation.tailrec
    private def resequence(seqnr: Long, cmd: Any, sdr: ActorRef) {
      if (seqnr == delivered + 1) {
        delivered = seqnr
        execute(cmd, sdr)
      } else {
        delayed += (seqnr ->(cmd, sender))
      }
      val eo = delayed.remove(delivered + 1)
      if (eo.isDefined) resequence(delivered + 1, eo.get._1, eo.get._2)
    }
  }

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


