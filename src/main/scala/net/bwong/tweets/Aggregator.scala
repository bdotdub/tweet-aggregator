package net.bwong.tweets

import com.twitter.finagle.Service
import com.twitter.util.Future
import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse, DefaultHttpRequest, QueryStringDecoder}
import org.jboss.netty.handler.codec.http.HttpMethod.GET
import org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1
import org.jboss.netty.util.CharsetUtil
import com.twitter.conversions.time._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{Http, Response}
import com.twitter.json.Json

object TwitterPage {
  def apply(screenName: String, page: Integer): Future[HttpResponse] = {
    val client: Service[HttpRequest, HttpResponse] = ClientBuilder()
      .codec(Http())
      .hosts(new InetSocketAddress("api.twitter.com", 443))
      .tlsWithoutValidation()
      .hostConnectionLimit(5)
      .retries(3)
      .build()
    var path = "/1/statuses/user_timeline.json?include_entities=true&count=200&screen_name=%s&page=%d".format(screenName, page)
    val request: HttpRequest = new DefaultHttpRequest(HTTP_1_1, GET, path)
    client(request)
  }
}

object ResponseToTweetTransformer {
  def apply(responses: Future[Seq[HttpResponse]]): List[Map[String, Any]] = {
    responses.get().toList flatMap { response =>
      val tweets = Json.parse(response.getContent.toString(CharsetUtil.UTF_8))
      tweets match {
        case tweets:List[Map[String, Any]] => tweets
        case _ => List[Map[String, Any]]()
      }
    }
  }
}

class Aggregator extends Service[HttpRequest, HttpResponse] {
  def apply(req: HttpRequest): Future[HttpResponse] = {
    val paramDecoder = new QueryStringDecoder(req.getUri())
    val screenName = paramDecoder.getParameters().get("screen_name").get(0)
    val fetchers = Seq(1, 2, 3, 4, 5, 6).map(page => TwitterPage(screenName, page))
    val allResponses = Future.collect(fetchers)
    val filteredTweets = ResponseToTweetTransformer(allResponses).map(tweet => tweet - "user")
    val response = Response()
    response.setStatusCode(200)
    response.setContentString(Json.build(filteredTweets).toString())
    Future(response)
  }
}
