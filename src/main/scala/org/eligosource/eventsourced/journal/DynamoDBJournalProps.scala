package org.eligosource.eventsourced.journal

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodb.{AmazonDynamoDBClient, AmazonDynamoDB}
import org.eligosource.eventsourced.core.{Journal, JournalProps}


case class DynamoDBJournalProps(journalTable: String, eventsourcedApp: String, key: String, secret: String, maxRetries: Int = 3, connectionTimeout: Int = 10000, socketTimeout: Int = 10000, asyncWriterCount:Int=16) extends JournalProps {


  private[journal] val clientConfig = {
    val c = new ClientConfiguration()
    c.setMaxConnections(asyncWriterCount)
    c.setMaxErrorRetry(maxRetries)
    c.setConnectionTimeout(connectionTimeout)
    c.setSocketTimeout(socketTimeout)
    c
  }

  lazy val dynamo: AmazonDynamoDB = new AmazonDynamoDBClient(new BasicAWSCredentials(key, secret), clientConfig)

  /**
   * Optional channel name.
   */
  def name: Option[String] = None

  /**
   * Optional dispatcher name.
   */
  def dispatcherName: Option[String] = None

  /**
   * Creates a [[org.eligosource.eventsourced.core.Journal]] actor instance.
   */
  def journal = new DynamoDBJournal(this)
}
