/*
 * Copyright 2012 Eligotech BV.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.eligosource.eventsourced.journal

import akka.actor.ActorSystem
import com.amazonaws.services.dynamodb.AmazonDynamoDB
import com.amazonaws.services.dynamodb.model.{AttributeValue, PutItemRequest}
import concurrent.{Await, Future}
import java.io.File
import java.nio.ByteBuffer
import org.eligosource.eventsourced.core._
import scala.concurrent.duration._
import akka.util.Timeout


/**
 * Configuration object for a [[http://code.google.com/p/leveldb/ LevelDB]] based journal. This
 * journal comes with different optimizations to choose from, as described at the methods
 *
 * - `withProcessorStructure`
 * - `withSequenceStructure`
 * - `withThrottledReplay`
 *
 * Journal actors can be created from a configuration object as follows:
 *
 * {{{
 *  import java.io.File
 *
 *  import akka.actor._
 *
 *  import org.eligosource.eventsourced.core.Journal
 *  import org.eligosource.eventsourced.journal.LeveldbJournalProps
 *
 *  implicit val system: ActorSystem = ...
 *
 *  val journalDir: File = ...
 *  val journal: ActorRef = Journal(LeveldbJournalProps(journalDir))
 * }}}
 *
 * @param dir Journal directory.
 * @param name Optional journal actor name.
 * @param dispatcherName Optional journal actor dispatcher name.
 * @param fsync `true` if every write is physically synced. Default is `false`.
 * @param checksum `true` if checksums are verified on read. Default is `false`.
 * @param processorStructured `true` if entries are primarily ordered by processor
 *                            id, `false` if entries are ordered by sequence number.  Default is `true`.
 * @param throttleAfter `0` if replay throttling is turned off, or a positive integer
 *                      to `throttleFor` after the specified number of replayed event messages.
 * @param throttleFor Suspend replay for specified duration after `throttleAfter`
 *                    replayed event messages.
 */
case class LeveldbJournalProps(
                                dir: File,
                                name: Option[String] = None,
                                dispatcherName: Option[String] = None,
                                fsync: Boolean = false,
                                checksum: Boolean = false,
                                processorStructured: Boolean = true,
                                throttleAfter: Int = 0,
                                throttleFor: FiniteDuration = 100 milliseconds) extends JournalProps {

  /**
   * Returns `false` if entries are primarily ordered by processor id,
   * `true` if entries are ordered by sequence number. Default is `false`.
   */
  def sequenceStructured: Boolean =
    !processorStructured

  /**
   * Returns `true` if replay will be throttled. Only for `processorStructured == true`.
   */
  def throttledReplay =
    processorStructured && (throttleAfter != 0)

  /**
   * Returns a new `LeveldbJournalProps` with specified journal actor name.
   */
  def withName(name: String) =
    copy(name = Some(name))

  /**
   * Returns a new `LeveldbJournalProps` with specified journal actor dispatcher name.
   */
  def withDispatcherName(dispatcherName: String) =
    copy(dispatcherName = Some(dispatcherName))

  /**
   * Returns a new `LeveldbJournalProps` with specified physical sync setting.
   */
  def withFsync(fsync: Boolean) =
    copy(fsync = fsync)

  /**
   * Returns a new `LeveldbJournalProps` with specified checksum verification setting.
   */
  def withChecksum(checksum: Boolean) =
    copy(checksum = checksum)

  /**
   * Returns a new `LeveldbJournalProps` with `processorStructured` set to `true` and
   * `sequenceStructured` set to `false`. With this setting, entries will be primarily
   * ordered by processor id.
   *
   * Pros:
   *
   * - efficient replay of input messages for all processors (batch replay)
   * - efficient replay of input messages for a single processor
   * - efficient replay of output messages
   *
   * Cons:
   *
   * - deletion of old entries requires full scan
   */
  def withProcessorStructure =
    copy(processorStructured = true)

  /**
   * Returns a new `LeveldbJournalProps` with `processorStructured` set to `false` and
   * `sequenceStructured` set to `true`. With this setting, entries will be ordered by
   * sequence number.
   *
   * Pros:
   *
   * - efficient replay of input messages for all processors (batch replay with optional lower bound)
   * - efficient replay of output messages
   * - efficient deletion of old entries
   *
   * Cons:
   *
   * - replay of input messages for a single processor requires full scan (with optional lower bound)
   */
  def withSequenceStructure =
    copy(processorStructured = false)

  /**
   * Returns a new `LeveldbJournalProps` with specified replay throttling settings. Can be used
   * to avoid growing of mailboxes of slow processors during replay. Only has effect if
   * `processorStructured == true`.
   */
  def withThrottledReplay(throttleAfter: Int, throttleFor: FiniteDuration = 100 milliseconds) =
    copy(throttleAfter = throttleAfter, throttleFor = throttleFor)

  def journal: Journal = {
    val key = sys.env("AWS_ACCESS_KEY_ID")
    val secret = sys.env("AWS_SECRET_ACCESS_KEY")
    val table = "eventsourced.dynamojournal.tests"
    val app = "test.app." + System.currentTimeMillis()
    val props: DynamoDBJournalProps = DynamoDBJournalProps(table, app, key, secret)
    DynamoDBJournal.createJournal(table)(props.dynamo)
    new DynamoDBJournal(props)
  }

  def dynamo: AmazonDynamoDB = {
    val key = sys.env("AWS_ACCESS_KEY_ID")
    val secret = sys.env("AWS_SECRET_ACCESS_KEY")
    val table = "eventsourced.dynamojournal.tests"
    val app = "test.app." + sys.env("TEST_APP")
    val props: DynamoDBJournalProps = DynamoDBJournalProps(table, app, key, secret)
    props.dynamo
  }

  def run {
    val d = dynamo
    val byts = new Array[Byte](10)
    val start = System.currentTimeMillis()
    implicit val system = ActorSystem("test").dispatcher
    (1 to 100000).grouped(32).foreach {
      group =>
        Await.result(Future.sequence {
          group.map {
            i => Future {
              val item = new java.util.HashMap[String, AttributeValue]
              item.put("id", new AttributeValue().withS(i.toString))
              item.put("sequence", new AttributeValue().withN(i.toString))
              item.put("event", new AttributeValue().withB(ByteBuffer.wrap(byts)))
              val put: PutItemRequest = new PutItemRequest().withTableName("eventsourced.dynamojournal.tests").withItem(item)
              d.putItem(put)
              println(s"put$i")
              if (i % 1000 == 0) {
                val elapsed = System.currentTimeMillis() - start
                println(s"$i $elapsed")
              }
            }
          }
        }, 10 seconds)
    }

  }
}

