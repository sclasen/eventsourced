package org.eligosource.eventsourced.journal

import akka.actor.ActorSystem
import java.io.File
import org.eligosource.eventsourced.core.Journal
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

class DynamoDBJournalSpec extends JournalSpec {


  def createJournal(journalDir: File)(implicit system: ActorSystem) = {
    val key = sys.env("AWS_ACCESS_KEY_ID")
    val secret = sys.env("AWS_SECRET_ACCESS_KEY")
    val table = "eventsourced.dynamojournal.tests"
    val app = System.currentTimeMillis().toString
    val props: DynamoDBJournalProps = DynamoDBJournalProps(table, app, key, secret)
    DynamoDBJournal.createJournal(table)(props.dynamo)
    Journal(props)
  }
}

class DynamoDBSerializationSpec extends WordSpec with MustMatchers {
  "dynamo" should {
    "serialize and deserialize correctly" in {
      val foo = "FOO".getBytes()
      val fooSAtt = DynamoDBJournal.bytesToS(foo)
      val fooBytes = DynamoDBJournal.SToBytes(fooSAtt)
      fooBytes must be(foo)
    }
  }
}
