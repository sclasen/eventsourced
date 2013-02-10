package org.eligosource.eventsourced.journal

import akka.actor.{Props, ActorRef, Actor}
import akka.util.Timeout
import collection.JavaConverters._
import com.amazonaws.auth.{AWS4Signer, BasicAWSCredentials}
import com.amazonaws.http.{HttpResponse => AWSHttpResponse, JsonResponseHandler, HttpMethodName}
import com.amazonaws.services.dynamodb.model._
import com.amazonaws.services.dynamodb.model.transform._
import com.amazonaws.transform.{JsonUnmarshallerContext, Unmarshaller, Marshaller}
import com.amazonaws.util.StringInputStream
import com.amazonaws.{AmazonWebServiceResponse, Request}
import concurrent.Future
import java.net.URI
import org.codehaus.jackson.JsonFactory
import scala.concurrent.duration._
import spray.can.client.{HttpDialog, DefaultHttpClient}
import spray.http.HttpHeaders._
import spray.http.HttpMethods._
import spray.http.HttpProtocols._
import spray.http.MediaTypes.CustomMediaType
import spray.http._
import spray.util._
import spray.client.HttpConduit

class DynamoDBClient(props: DynamoDBJournalProps) extends Actor {

  implicit val timeout = Timeout(10 seconds)

  val log = context.system.log

  val endpoint = "dynamodb.us-east-1.amazonaws.com"
  val endpointUri = new URI(s"http://$endpoint")

  val connection = DefaultHttpClient(context.system)
  //def dialog = HttpDialog(connection, "dynamodb.us-east-1.amazonaws.com")//, 443, SslEnabled)
  val conduit = context.actorOf(
      props = Props(new HttpConduit(connection, "dynamodb.us-east-1.amazonaws.com", 443, true))
    )

  val pipeline = HttpConduit.sendReceive(conduit)

  val credentials = new BasicAWSCredentials(props.key, props.secret)
  val signer = new AWS4Signer()

  implicit val batchWriteM: Marshaller[Request[BatchWriteItemRequest], BatchWriteItemRequest] = new BatchWriteItemRequestMarshaller()
  implicit val batchWriteU = BatchWriteItemResultJsonUnmarshaller.getInstance()
  implicit val putItemM = new PutItemRequestMarshaller()
  implicit val putItemU = PutItemResultJsonUnmarshaller.getInstance()
  implicit val delItemM = new DeleteItemRequestMarshaller()
  implicit val delItemU = DeleteItemResultJsonUnmarshaller.getInstance()
  implicit val batchGetM = new BatchGetItemRequestMarshaller()
  implicit val batchGetU = BatchGetItemResultJsonUnmarshaller.getInstance()
  val jsonFactory = new JsonFactory()


  val `application/x-amz-json-1.0` = CustomMediaType("application/x-amz-json-1.0")


  def receive = {
    case awsWrite: BatchWriteItemRequest => sendBatchWrite(awsWrite, sender)
    case awsGet: BatchGetItemRequest =>
      val s = sender
      sendBatchGet(awsGet, s)
    case awsDel: DeleteItemRequest => sendDelete(awsDel, sender)
  }


  def sendBatchWrite(awsWrite: BatchWriteItemRequest, snd: ActorRef) {
    val req = pipeline(request(awsWrite)).map(response[BatchWriteItemResult])
    req.onSuccess {
      case result =>
        if (result.getUnprocessedItems.size() == 0) snd !()
        else sendUnprocessedItems(result, snd)
    }
    req.onFailure {
      case e: Exception =>
        log.error("sendBatchWrite!!!" + e.toString)
        snd ! e
    }
  }

  def sendUnprocessedItems(result: BatchWriteItemResult, snd: ActorRef) {
    result.getUnprocessedItems.asScala.foreach {
      case (t, u) =>
        log.error("UNPROCESSED!")
        val puts = Future.sequence {
          u.asScala.map {
            w =>
              val p = new PutItemRequest().withTableName(props.journalTable).withItem(w.getPutRequest.getItem)
              pipeline(request(p)).map(response[PutItemResult])
          }
        }
        puts.onSuccess {
          case results => snd !()
        }
        puts.onFailure {
          case e: Exception =>
            log.error("sendUnprocessed!!!" +  e.toString) //todo propagate failures
            snd ! e

        }
    }
  }

  ///todo all BatchGetItem need to chek and retry for unprocessed keys before mapBatch-ing
  def sendBatchGet(awsGet: BatchGetItemRequest, snd: ActorRef) {
    val req = pipeline(request(awsGet)).map(response[BatchGetItemResult])
    req.onSuccess {
      case result =>
        snd ! result
    }
    req.onFailure {
      case e: Exception =>
        log.error("sendBatchGet!!!" +  e.toString) //todo propagate failures
        snd ! e

    }
  }

  def sendDelete(awsDel: DeleteItemRequest, snd: ActorRef) {
    val req = pipeline(request(awsDel)).map(response[DeleteItemResult])
    req.onSuccess {
      case result => snd !()
    }
    req.onFailure {
      case e: Exception =>
        log.error("sendDeltet!!!" +  e.toString) //todo propagate failures
        snd ! e

    }
  }

  def request[T](t: T)(implicit marshaller: Marshaller[Request[T], T]): HttpRequest = {
    val awsReq = marshaller.marshall(t)
    awsReq.setEndpoint(endpointUri)
    val body = awsReq.getContent.asInstanceOf[StringInputStream].getString
    signer.sign(awsReq, credentials)
    var path: String = awsReq.getResourcePath
    if (path == "") path = "/"
    val request = HttpRequest(awsReq.getHttpMethod, path, headers(awsReq), HttpBody(`application/x-amz-json-1.0`, body), `HTTP/1.1`)
    request
  }

  def response[T](response: HttpResponse)(implicit unmarshaller: Unmarshaller[T, JsonUnmarshallerContext]): T = try {
    val awsResp = new AWSHttpResponse(null, null)
    awsResp.setContent(new StringInputStream(response.entity.asString))
    awsResp.setStatusCode(response.status.value)
    awsResp.setStatusText(response.status.defaultMessage)
    val handler = new JsonResponseHandler[T](unmarshaller)
    val handle: AmazonWebServiceResponse[T] = handler.handle(awsResp)   ///todo how do 4xx work
    val resp = handle.getResult
    resp
  } catch {
    case e: Exception => log.error(e, e.getStackTraceString); throw e
  }

  def headers(req: Request[_]): List[HttpHeader] = {
    req.getHeaders.asScala.map {
      case (k, v) =>
        RawHeader(k, v)
    }.toList
  }

  implicit def bridgeMethods(m: HttpMethodName): HttpMethod = m match {
    case HttpMethodName.POST => POST
    case HttpMethodName.GET => GET
    case HttpMethodName.PUT => PUT
    case HttpMethodName.DELETE => DELETE
    case HttpMethodName.HEAD => HEAD
  }

}
