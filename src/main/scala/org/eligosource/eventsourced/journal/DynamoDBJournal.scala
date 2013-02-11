package org.eligosource.eventsourced.journal

import DynamoDBJournal._
import akka.actor._
import akka.pattern._
import akka.util._
import annotation.tailrec
import collection.JavaConverters._
import collection.immutable.IndexedSeq
import collection.immutable.TreeMap
import com.amazonaws.services.dynamodb.AmazonDynamoDB
import com.amazonaws.services.dynamodb.model._
import com.amazonaws.services.dynamodb.model.{Key => DynamoKey}
import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Timer
import java.nio.ByteBuffer
import java.util.Collections
import java.util.concurrent.TimeUnit
import java.util.{List => JList, Map => JMap, HashMap => JHMap}
import org.eligosource.eventsourced.core.Channel.Deliver
import org.eligosource.eventsourced.core.Journal._
import org.eligosource.eventsourced.core.{Serialization, Message}
import org.eligosource.eventsourced.core.Journal.Looped
import org.eligosource.eventsourced.core.Journal.ReplayOutMsgs
import org.eligosource.eventsourced.core.Journal.WriteAck
import org.eligosource.eventsourced.core.Journal.BatchDeliverOutMsgs
import org.eligosource.eventsourced.core.Journal.Written
import org.eligosource.eventsourced.core.Journal.DeleteOutMsg
import org.eligosource.eventsourced.core.Journal.WriteOutMsg
import org.eligosource.eventsourced.core.Journal.ReplayInMsgs
import org.eligosource.eventsourced.core.Message
import org.eligosource.eventsourced.core.Journal.Loop
import org.eligosource.eventsourced.core.Journal.WriteInMsg
import org.eligosource.eventsourced.core.Journal.SetCommandListener
import org.eligosource.eventsourced.core.Journal.BatchReplayInMsgs
import concurrent.{Future, Await}

/**
 * Current status:
 *
 *
 * Needs a strategy for storing Messages with size > 64k.
 *
 */
class DynamoDBJournal(props: DynamoDBJournalProps) extends Actor {


  val serialization = Serialization(context.system)

  implicit def msgToBytes(msg: Message): Array[Byte] = serialization.serializeMessage(msg)

  implicit def msgFromBytes(bytes: Array[Byte]): Message = serialization.deserializeMessage(bytes)

  val channelMarker = Array(1.toByte)
  val countMarker = Array(1.toByte)

  val log = context.system.log
  log.debug("new Journal")

  def counterAtt(cntr: Long) = S(props.eventsourcedApp + Counter + cntr)

  def counterKey(cntr: Long) =
    new DynamoKey()
      .withHashKeyElement(counterAtt(cntr))

  implicit val ctx = context.system.dispatcher

  implicit val timeout = props.operationtTmeout

  val dynamo = new DynamoDBClient(props, context)

  protected def storedCounter: Long = {
    val start = Long.MaxValue
    val counter = Await.result(findStoredCounter(start), props.operationtTmeout.duration)
    log.debug(s"found stored counter $counter")
    counter
  }

  private def findStoredCounter(max: Long): Future[Long] = {
    val candidates = candidateKeys(max)
    val ka = new KeysAndAttributes().withKeys(candidates.values.toSeq: _*).withConsistentRead(true)
    val tables = Collections.singletonMap(props.journalTable, ka)
    val get = new BatchGetItemRequest().withRequestItems(tables)
    dynamo.sendBatchGet(get).flatMap{
      res =>
        val batch = mapBatch(res.getResponses.get(props.journalTable))
        val counters: List[Long] = candidates.map {
          ///find the counters associated with any found keys
          case (cnt, key) => Option(batch.get(key.getHashKeyElement)).map(_ => cnt)
        }.flatten.toList
        if (counters.size == 0) Future(0) //no counters found
        else if (counters.size == 1 && counters(0) == 1) Future(1) //one counter found
        else if (endsSequentially(counters)) Future(counters.last) // last 2 counters found are sequential so last one is highest
        else findStoredCounter(counters.last)
    }
  }

  def endsSequentially(counters: List[Long]): Boolean = {
    val two = counters.takeRight(2)
    two match {
      case a :: b :: Nil if a + 1 == b => true
      case _ => false
    }
  }


  def candidateKeys(max: Long): TreeMap[Long, DynamoKey] = {
    val increment: Long = max / 100
    (Stream.iterate(1L, 100)(i => i + increment)).map {
      i =>
        i -> counterKey(i)
    }.foldLeft(TreeMap.empty[Long, DynamoKey]) {
      case (tm, (cnt, key)) => tm + (cnt -> key)
    }
  }

  ///todo all BatchGetItem need to chek and retry for unprocessed keys before mapBatch-ing
  private def replayOut(r: ReplayOutMsgs, replayTo: Long, p: (Message) => Unit) {
    val from = r.fromSequenceNr
    val msgs = (replayTo - r.fromSequenceNr).toInt + 1
    log.debug(s"replayingOut from ${from} for up to ${msgs}")
    Stream.iterate(r.fromSequenceNr, msgs)(_ + 1)
      .map(l => new DynamoKey().withHashKeyElement(outKey(r.channelId, l))).grouped(100).foreach {
      keys =>
        log.debug("replayingOut")
        val ka = new KeysAndAttributes().withKeys(keys.asJava).withConsistentRead(true)
        val get = new BatchGetItemRequest().withRequestItems(Collections.singletonMap(props.journalTable, ka))
        val resp = Await.result(dynamo.sendBatchGet(get),props.operationtTmeout.duration)
        val batchMap = mapBatch(resp.getResponses.get(props.journalTable))
        keys.foreach {
          key =>
            Option(batchMap.get(key.getHashKeyElement)).foreach {
              item =>
                p(msgFromBytes(item.get(Data).getB.array()))
            }
        }
    }
  }

  private def replayIn(r: ReplayInMsgs, replayTo: Long, processorId: Int, p: (Message) => Unit) {
    val from = r.fromSequenceNr
    val msgs = (replayTo - r.fromSequenceNr).toInt + 1
    log.debug(s"replayingIn from ${from} for up to ${msgs}")
    Stream.iterate(r.fromSequenceNr, msgs)(_ + 1)
      .map(l => new DynamoKey().withHashKeyElement(inKey(r.processorId, l))).grouped(100).foreach {
      keys =>
        log.debug("replayingIn")
        keys.foreach(k => log.debug(k.toString))
        val ka = new KeysAndAttributes().withKeys(keys.asJava).withConsistentRead(true)
        val get = new BatchGetItemRequest().withRequestItems(Collections.singletonMap(props.journalTable, ka))
        val resp = Await.result(dynamo.sendBatchGet(get), props.operationtTmeout.duration)
        val batchMap = mapBatch(resp.getResponses.get(props.journalTable))
        val messages = keys.map {
          key =>
            Option(batchMap.get(key.getHashKeyElement)).map {
              item =>
                msgFromBytes(item.get(Data).getB.array())
            }
        }.flatten
        log.debug(s"found ${messages.size}")
        confirmingChannels(processorId, messages).foreach(p)
    }
  }

  def confirmingChannels(processorId: Int, messages: Stream[Message]): Stream[Message] = {
    if (messages.isEmpty) messages
    else {
      val keys = messages.map {
        message =>
          new DynamoKey()
            .withHashKeyElement(ackKey(processorId, message.sequenceNr))
      }

      val ka = new KeysAndAttributes().withKeys(keys.asJava).withConsistentRead(true)
      val get = new BatchGetItemRequest().withRequestItems(Collections.singletonMap(props.journalTable, ka))
      val response = Await.result(dynamo.sendBatchGet(get) ,props.operationtTmeout.duration)
      val batchMap = mapBatch(response.getResponses.get(props.journalTable))
      val acks = keys.map {
        key =>
          Option(batchMap.get(key.getHashKeyElement)).map {
            _.keySet().asScala.filter(!DynamoKeys.contains(_)).map(_.toInt).toSeq
          }
      }

      messages.zip(acks).map {
        case (message, Some(chAcks)) => message.copy(acks = chAcks.filter(_ != -1))
        case (message, None) => message
      }

    }
  }

  def mapBatch(b: BatchResponse) = {
    val map = new JHMap[AttributeValue, JMap[String, AttributeValue]]
    b.getItems.iterator().asScala.foreach {
      item => map.put(item.get(Id), item)
    }
    map
  }


  def put(key: DynamoKey, message: Array[Byte]): PutRequest = {
    val item = new JHMap[String, AttributeValue]
    log.debug(s"put:  ${key.toString}")
    item.put(Id, key.getHashKeyElement)
    item.put(Data, B(message))
    new PutRequest().withItem(item)
  }

  def putAck(ack: WriteAck): PutRequest = {
    val item = new JHMap[String, AttributeValue]
    item.put(Id, ack.getHashKeyElement)
    item.put(ack.channelId.toString, B(channelMarker))
    new PutRequest().withItem(item)
  }


  def S(value: String): AttributeValue = new AttributeValue().withS(value)

  def N(value: Long): AttributeValue = new AttributeValue().withN(value.toString)

  def NS(value: Long): AttributeValue = new AttributeValue().withNS(value.toString)

  def B(value: Array[Byte]): AttributeValue = new AttributeValue().withB(ByteBuffer.wrap(value))

  def UB(value: Array[Byte]): AttributeValueUpdate = new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(B(value))

  def inKey(procesorId: Int, sequence: Long) = S(str(props.eventsourcedApp, "IN-", procesorId, "-", sequence)) //dont remove those dashes or else keys will be funky

  def outKey(channelId: Int, sequence: Long) = S(str(props.eventsourcedApp, "OUT-", channelId, "-", sequence))

  def ackKey(processorId: Int, sequence: Long) = S(str(props.eventsourcedApp, "ACK-", processorId, "-", sequence))

  def str(ss: Any*): String = ss.foldLeft(new StringBuilder)(_.append(_)).toString()

  implicit def inToDynamoKey(cmd: WriteInMsg): DynamoKey =
    new DynamoKey()
      .withHashKeyElement(inKey(cmd.processorId, cmd.message.sequenceNr))

  implicit def outToDynamoKey(cmd: WriteOutMsg): DynamoKey =
    new DynamoKey()
      .withHashKeyElement(outKey(cmd.channelId, cmd.message.sequenceNr))

  implicit def delToDynamoKey(cmd: DeleteOutMsg): DynamoKey =
    new DynamoKey().
      withHashKeyElement(outKey(cmd.channelId, cmd.msgSequenceNr))

  implicit def ackToDynamoKey(cmd: WriteAck): DynamoKey =
    new DynamoKey()
      .withHashKeyElement(ackKey(cmd.processorId, cmd.ackSequenceNr))

  implicit def replayInToDynamoKey(cmd: ReplayInMsgs): DynamoKey =
    new DynamoKey().withHashKeyElement(inKey(cmd.processorId, cmd.fromSequenceNr))

  implicit def replayOutToDynamoKey(cmd: ReplayOutMsgs): DynamoKey =
    new DynamoKey().withHashKeyElement(outKey(cmd.channelId, cmd.fromSequenceNr))


  val deadLetters = context.system.deadLetters
  val resequencer: ActorRef = context.actorOf(Props(new Resequencer),"resequencer")

  val listener = context.system.actorOf(Props(new Actor {
    def receive = {
      case d: DeadLetter ⇒ log.debug(d.toString())
    }
  }))

  context.system.eventStream.subscribe(listener, classOf[DeadLetter])


  val w: IndexedSeq[ActorRef] = (1 to props.asyncWriterCount).map {
    i => context.actorOf(Props(new Writer(i)), s"writer$i")
  }

  val writers: Vector[ActorRef] = Vector(w: _*)

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
      case t => resequencer tell((ctr, WriteFailed(cmd, t)), sdr)
    }
    _counterResequencer += 1L
  }

  def asyncResequence(cmd: Any) {
    resequencer forward(counterResequencer, cmd)
    _counterResequencer += 1L
  }

  case class SnapshottedReplay(replayCmd: Any, toSequencerNr: Long)

  case class WriteFailed(cmd: Any, cause: Throwable)

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
      asyncResequence(SnapshottedReplay(cmd, counter))
    }
    case cmd: ReplayInMsgs => {
      asyncResequence(SnapshottedReplay(cmd, counter))
    }
    case cmd: ReplayOutMsgs => {
      asyncResequence(SnapshottedReplay(cmd, counter))
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
    log.debug("prestart" + _counter)
  }

  class Writer(idx:Int) extends Actor {

    val timer = Metrics.newTimer(classOf[Writer], "all-writes", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
    val timerIn = Metrics.newTimer(classOf[Writer], "in-writes", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
    val timerOut = Metrics.newTimer(classOf[Writer], "out-writes", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
    val timerAck = Metrics.newTimer(classOf[Writer], "ack-writes", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)

    def timed[T](timers: Timer*)(block: => T): T = {
      val ctx = timers.map(_.time())
      val t = block
      ctx.foreach(_.stop())
      t
    }

    def receive = {
      case (nr, cmd: WriteInMsg) => {
        executeWriteInMsg(cmd)(sender)
        //sender !()
      }
      case (nr, cmd: WriteOutMsg) => {
        executeWriteOutMsg(cmd)(sender)
        //sender !()
      }
      case (nr, cmd: WriteAck) => {
        executeWriteAck(cmd)(sender)
        // sender !()
      }
      case (nr, cmd: DeleteOutMsg) => {
        executeDeleteOutMsg(cmd)(sender)
        //sender !()
      }
    }

    override def preRestart(reason: Throwable, message: Option[Any]) {
      sender ! Status.Failure(reason)
    }

    def executeDeleteOutMsg(cmd: DeleteOutMsg)(snd:ActorRef) {
      val del: DeleteItemRequest = new DeleteItemRequest().withTableName(props.journalTable).withKey(cmd)
      val delete = dynamo.sendDelete(del)
      delete.onSuccess {
        case _ => snd !()
      }
      delete.onFailure{
        case e:Exception=> snd ! WriteFailed(cmd, e)
      }
    }

    def executeWriteOutMsg(cmd: WriteOutMsg)(s: ActorRef) {
      val ack = {
        if (cmd.ackSequenceNr != SkipAck)
          putAck(WriteAck(cmd.ackProcessorId, cmd.channelId, cmd.ackSequenceNr))
        else
        //write a -1 to acks so we can be assured of non-nulls on the batch get in replay
          putAck(WriteAck(cmd.ackProcessorId, -1, cmd.ackSequenceNr))
      }

      log.debug(s"batch out with counter ${cmd.message.sequenceNr}")
      timed(timer, timerOut) {
        batchWrite(cmd,
          put(cmd, cmd.message.clearConfirmationSettings),
          put(counterKey(cmd.message.sequenceNr), countMarker),
          ack
        )(s)
      }
    }

    def executeWriteInMsg(cmd: WriteInMsg)(s: ActorRef) {
      log.debug(s"batch in with counter ${cmd.message.sequenceNr}")
      timed(timer, timerIn) {
        batchWrite(cmd,
          put(cmd, cmd.message.clearConfirmationSettings),
          put(counterKey(cmd.message.sequenceNr), countMarker)
        )(s)
      }
    }

    def executeWriteAck(cmd: WriteAck)(s: ActorRef) {
      timed(timer, timerAck) {
        batchWrite(cmd,putAck(cmd))(s)
      }
    }

    def batchWrite(cmd:Any, puts: PutRequest*)(s: ActorRef) {
      log.debug("batchWrite")
      val write = new JHMap[String, JList[WriteRequest]]
      val writes = puts.map(new WriteRequest().withPutRequest(_)).asJava
      write.put(props.journalTable, writes)
      val batch = new BatchWriteItemRequest().withRequestItems(write)
      val req = dynamo.sendBatchWrite(batch)
      req.onSuccess {
        case _ => s !()
      }
      req.onFailure{
        case e:Exception=> s! WriteFailed(cmd, e)
      }

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
        target tell(Looped(msg), sdr)
      }
      case SnapshottedReplay(BatchReplayInMsgs(replays), toSeq) => {
        executeBatchReplayInMsgs(replays, toSeq, (msg, target) => target tell(Written(msg), deadLetters), sdr)
      }
      case SnapshottedReplay(cmd: ReplayInMsgs, toSeq) => {
        executeReplayInMsgs(cmd, toSeq, msg => cmd.target tell(Written(msg), deadLetters), sdr)
      }
      case SnapshottedReplay(cmd: ReplayOutMsgs, toSeq) => {
        executeReplayOutMsgs(cmd, toSeq, msg => cmd.target tell(Written(msg), deadLetters), sdr)
      }
      case BatchDeliverOutMsgs(channels) => {
        channels.foreach(_ ! Deliver)
        sdr ! DeliveryDone
      }
      case e: WriteFailed => {
        context.system.eventStream.publish(e)
      }
    }


    def executeBatchReplayInMsgs(cmds: Seq[ReplayInMsgs], replayTo: Long, p: (Message, ActorRef) => Unit, sender: ActorRef) {
      cmds.foreach(cmd => replayIn(cmd, replayTo, cmd.processorId, p(_, cmd.target)))
      sender ! ReplayDone
    }

    def executeReplayInMsgs(cmd: ReplayInMsgs, replayTo: Long, p: (Message) => Unit, sender: ActorRef) {
      replayIn(cmd, replayTo, cmd.processorId, p)
      sender ! ReplayDone
    }

    def executeReplayOutMsgs(cmd: ReplayOutMsgs, replayTo: Long, p: (Message) => Unit, sender: ActorRef) {
      replayOut(cmd, replayTo, p)
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

  val Id = "key"
  val Data = "data"
  val Counter = "COUNTER"
  val DynamoKeys = Set(Id)

  val hashKey = new KeySchemaElement().withAttributeName("key").withAttributeType("S")
  val schema = new KeySchema().withHashKeyElement(hashKey)


  def createJournal(table: String)(implicit dynamo: AmazonDynamoDB) {
    if (!dynamo.listTables(new ListTablesRequest()).getTableNames.contains(table)) {
      dynamo.createTable(new CreateTableRequest(table, schema).withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(128).withWriteCapacityUnits(128)))
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


