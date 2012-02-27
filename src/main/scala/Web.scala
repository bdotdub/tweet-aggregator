import com.twitter.finagle.{WriteException, Service}
import com.twitter.util.{Throw, Try, Future}
import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse, DefaultHttpRequest}
import org.jboss.netty.handler.codec.http.HttpMethod.GET
import org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1
import org.jboss.netty.util.CharsetUtil
import com.twitter.conversions.time._
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{Http, Response}
import com.twitter.finagle.service.{RetryPolicy, Backoff}
import com.twitter.json.Json
import util.Properties

object Web {
  def main(args: Array[String]) {
    val port = Properties.envOrElse("PORT", "8080").toInt
    println("Starting on port:"+port)
    ServerBuilder()
      .codec(Http())
      .name("hello-server")
      .bindTo(new InetSocketAddress(port))
      .build(new Hello)
  }
}

object TwitterPage {
  def apply(page: Integer): Future[List[Map[String, Any]]] = {
    val client: Service[HttpRequest, HttpResponse] = ClientBuilder()
      .codec(Http())
      .hosts(new InetSocketAddress("api.twitter.com", 443))
      .tlsWithoutValidation()
      .hostConnectionLimit(5)
      .retries(3)
      .build()
    var path = "/1/statuses/user_timeline.json?include_entities=true&count=200&screen_name=bdotdub&page=%d" format page
    val request: HttpRequest = new DefaultHttpRequest(HTTP_1_1, GET, path)
    val response = client(request)
    val tweets = Json.parse(response.get().getContent.toString(CharsetUtil.UTF_8))
    tweets match {
      case tweets:List[Map[String, Any]] => Future(tweets)
      case _ => Future(List[Map[String, Any]]())
    }
  }
}

class Hello extends Service[HttpRequest, HttpResponse] {
  def apply(req: HttpRequest): Future[HttpResponse] = {
    val fetchers = List(1, 2, 3, 4, 5, 6).map(page => TwitterPage(page))
    val allTweets = Future.collect(fetchers.toSeq)
    val filteredTweets = allTweets.get().map(tweet => tweet - "user")
    val response = Response()
    response.setStatusCode(200)
    response.setContentString(Json.build(filteredTweets).toString())
    Future(response)
  }
}
