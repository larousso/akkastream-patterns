package controllers

import javax.inject.Inject
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.FlowShape
import akka.stream.scaladsl.{Balance, Flow, Framing, GraphDSL, Merge, Source}
import akka.util.ByteString
import play.api.Logger
import play.api.libs.json.{JsArray, JsObject, Json}
import play.api.libs.streams.Accumulator
import play.api.libs.ws.WSClient
import play.api.mvc._

import scala.concurrent.ExecutionContext
import scala.util.Failure

class VikingsController @Inject()(executionContext: ExecutionContext, wsClient: WSClient, cc: ControllerComponents) extends AbstractController(cc) {

  implicit val ec = executionContext

  val sourceBodyParser = BodyParser("CSV BodyParser") { _ =>
    Accumulator.source[ByteString].map(Right.apply)
  }

  case class ImportStatus(nbSuccess: Int = 0, nbError: Int = 0)

  object ImportStatus {
    implicit val format = Json.format[ImportStatus]
  }

  // curl -XDELETE http://localhost:9200/vikings
  def csvUpload(): Action[Source[ByteString, _]] = Action(sourceBodyParser) { req =>
    import ImportStatus._

    val body: Source[ByteString, _] = req.body

    val servers = List("localhost:9200", "localhost:9201")
    val index = Json.obj("index" -> Json.obj("_index" -> "vikings", "_type" -> "vikings"))

    def getBulk(bulk: Seq[JsObject]): String = {
      bulk.flatMap(j => List(index, j)).map(Json.stringify).mkString("", "\n", "\n")
    }

    //curl -X POST --data-binary @./conf/vikings.csv -H "Content-Type: text/csv" http://localhost:9000/vikingslb
    val response = body
      .via(Framing.delimiter(ByteString("\n"), 1000, true))
      .map(_.utf8String)
      .drop(1)
      .map(_.split(";").toList)
      .collect {
        case name :: place :: Nil => Json.obj("name" -> name, "place" -> place)
      }
      .grouped(5)
      .via(loadBalancing(servers) { server =>
        Logger.debug(s"$server for flow")
        Flow[Seq[JsObject]].mapAsync(1) { bulk =>
          val strBulk = getBulk(bulk)
          Logger.debug(s"$server -> $strBulk")
          wsClient.url(s"http://$server/_bulk")
            .withHttpHeaders("Content-Type" -> "application/x-ndjson")
            .post(strBulk)
        }
      })
      .map{ resp => resp.json.as[JsObject] }
      .map { resp =>
        val errors = (resp \ "items").as[JsArray].value.map(i => (i \ "error").asOpt[JsObject].exists(_ => true))
        ImportStatus(nbSuccess = errors.count(e => !e), nbError = errors.count(identity))
      }
      .fold(ImportStatus()) { (s, e) => s.copy(nbSuccess = s.nbSuccess + e.nbSuccess, nbError = s.nbError + e.nbError) }
      .map(status => Json.stringify(Json.toJson(status)))
      .map(ByteString.apply)
      .watchTermination() { (_, d) =>
        d.onComplete {
          case Failure(exception) =>
            Logger.error("Oups", exception)
          case _ =>
        }
        d
      }

    Ok.chunked(response).as("application/json")
  }

  def loadBalancing[In,Out](servers: List[String])(flow: String => Flow[In, Out, NotUsed]): Flow[In, Out, NotUsed] =
    Flow.fromGraph { GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val parallelism = servers.size
      val merge = b.add(Merge[Out](parallelism))
      val balance = b.add(Balance[In](parallelism))

      for ((server, i) <- servers.zipWithIndex) {
        Logger.info(s"Server $server $i")
        balance.out(i) ~> flow(server).async ~> merge.in(i)
      }

      FlowShape(balance.in, merge.out)
    }}

}
