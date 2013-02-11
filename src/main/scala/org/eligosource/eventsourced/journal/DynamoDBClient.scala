package org.eligosource.eventsourced.journal

import akka.actor.{ActorContext, Props}
import akka.util.Timeout
import collection.JavaConverters._
import com.amazonaws.auth.{AWS4Signer, BasicAWSCredentials}
import com.amazonaws.http.{HttpResponse => AWSHttpResponse, JsonErrorResponseHandler, JsonResponseHandler, HttpMethodName}
import com.amazonaws.services.dynamodb.model._
import com.amazonaws.services.dynamodb.model.transform._
import com.amazonaws.transform.{JsonErrorUnmarshaller, JsonUnmarshallerContext, Unmarshaller, Marshaller}
import com.amazonaws.util.StringInputStream
import com.amazonaws.util.json.JSONObject
import com.amazonaws.{DefaultRequest, AmazonServiceException, AmazonWebServiceResponse, Request}
import concurrent.Future
import java.net.URI
import java.util.{List => JList, Map => JMap, HashMap => JHMap}
import org.codehaus.jackson.JsonFactory
import spray.can.client.DefaultHttpClient
import spray.client.HttpConduit
import spray.http.HttpHeaders._
import spray.http.HttpMethods._
import spray.http.HttpProtocols._
import spray.http.MediaTypes.CustomMediaType
import spray.http._





class DynamoDBClient(props: DynamoDBJournalProps, context: ActorContext) {



  implicit val timeout = props.operationtTmeout
  implicit val excn = context.system.dispatcher

  val log = context.system.log
  val serviceName = "dynamodb"
  val endpoint = "dynamodb.us-east-1.amazonaws.com"
  val endpointUri = new URI(s"https://$endpoint")

  val connection = DefaultHttpClient(context.system)
  val conduit = context.actorOf(
    props = Props(new HttpConduit(connection, endpoint))
  )

  val pipeline = HttpConduit.sendReceive(conduit)

  val credentials = new BasicAWSCredentials(props.key, props.secret)
  val signer = new AWS4Signer()
  signer.setServiceName(serviceName)

  implicit val batchWriteM: Marshaller[Request[BatchWriteItemRequest], BatchWriteItemRequest] = new BatchWriteItemRequestMarshaller()
  implicit val batchWriteU = BatchWriteItemResultJsonUnmarshaller.getInstance()
  implicit val putItemM = new PutItemRequestMarshaller()
  implicit val putItemU = PutItemResultJsonUnmarshaller.getInstance()
  implicit val delItemM = new DeleteItemRequestMarshaller()
  implicit val delItemU = DeleteItemResultJsonUnmarshaller.getInstance()
  implicit val batchGetM = new BatchGetItemRequestMarshaller()
  implicit val batchGetU = BatchGetItemResultJsonUnmarshaller.getInstance()
  val jsonFactory = new JsonFactory()

  val exceptionUnmarshallers: JList[JsonErrorUnmarshaller] = List(
    new LimitExceededExceptionUnmarshaller(),
    new InternalServerErrorExceptionUnmarshaller(),
    new ProvisionedThroughputExceededExceptionUnmarshaller(),
    new ResourceInUseExceptionUnmarshaller(),
    new ConditionalCheckFailedExceptionUnmarshaller(),
    new ResourceNotFoundExceptionUnmarshaller(),
    new JsonErrorUnmarshaller()).toBuffer.asJava


  val `application/x-amz-json-1.0` = CustomMediaType("application/x-amz-json-1.0")


  def sendBatchWrite(awsWrite: BatchWriteItemRequest): Future[(BatchWriteItemResult, List[PutItemResult])] = {
    pipeline(request(awsWrite)).map(response[BatchWriteItemResult]).flatMap {
      result => sendUnprocessedItems(result)
    }
  }

  def sendUnprocessedItems(result: BatchWriteItemResult): Future[(BatchWriteItemResult, List[PutItemResult])] = {
    Future.sequence {
      result.getUnprocessedItems.get(props.journalTable).asScala.map {
        w =>
          val p = new PutItemRequest().withTableName(props.journalTable).withItem(w.getPutRequest.getItem)
          pipeline(request(p)).map(response[PutItemResult])
      }
    }.map(puts => result -> puts.toList)
  }

  ///todo all BatchGetItem need to chek and retry for unprocessed keys before mapBatch-ing
  def sendBatchGet(awsGet: BatchGetItemRequest): Future[BatchGetItemResult] = {
    pipeline(request(awsGet)).map(response[BatchGetItemResult])
  }

  def sendDelete(awsDel: DeleteItemRequest):Future[DeleteItemResult]= {
    pipeline(request(awsDel)).map(response[DeleteItemResult])
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

  def response[T](response: HttpResponse)(implicit unmarshaller: Unmarshaller[T, JsonUnmarshallerContext]): T = {
    val req = new DefaultRequest[T](serviceName)
    val awsResp = new AWSHttpResponse(req, null)
    awsResp.setContent(new StringInputStream(response.entity.asString))
    awsResp.setStatusCode(response.status.value)
    awsResp.setStatusText(response.status.defaultMessage)
    if (awsResp.getStatusCode == 200) {
      val handler = new JsonResponseHandler[T](unmarshaller)
      val handle: AmazonWebServiceResponse[T] = handler.handle(awsResp) ///todo how do 4xx work
      val resp = handle.getResult
      resp
    } else {
      response.headers.foreach {
        h => awsResp.addHeader(h.name, h.value)
      }
      val errorResponseHandler = new JsonErrorResponseHandler(exceptionUnmarshallers.asInstanceOf[JList[Unmarshaller[AmazonServiceException, JSONObject]]])
      throw errorResponseHandler.handle(awsResp)
    }
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
