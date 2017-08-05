package controllers

import javax.inject.Inject

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.FlowShape
import akka.stream.scaladsl.{Balance, Flow, Framing, GraphDSL, Merge, Source}
import akka.util.ByteString
import play.api.libs.json.{JsObject, Json}
import play.api.libs.streams.Accumulator
import play.api.libs.ws.WSClient
import play.api.mvc._

import scala.concurrent.ExecutionContext

class VikingsController @Inject()(executionContext: ExecutionContext, wsClient: WSClient, cc: ControllerComponents) extends AbstractController(cc) {

  implicit val ec = executionContext

  val sourceBodyParser = BodyParser("CSV BodyParser") { _ =>
    Accumulator.source[ByteString].map(Right.apply)
  }

  case class ImportStatus(nbSuccess: Int = 0, nbError: Int = 0)

  object ImportStatus {
    implicit val format = Json.format[ImportStatus]
  }

  def csvUpload() = Action(sourceBodyParser) { req =>
    import ImportStatus._

    val body: Source[ByteString, _] = req.body

    val servers = List("server1", "server2")
    val index1 = Json.obj("index" -> Json.obj("_index" -> "vikings", "_type" -> "vikings1"))
    val index2 = Json.obj("index" -> Json.obj("_index" -> "vikings", "_type" -> "vikings2"))

    val response = body
      .via(Framing.delimiter(ByteString("\n"), 1000, true))
      .map(_.utf8String)
      .drop(1)
      .map(_.split(";").toList)
      .collect {
        case name :: place :: Nil => Json.obj("name" -> name, "place" -> place)
      }
      .grouped(5)
      .via(loadBalancing(servers){ server =>
        Flow[Seq[JsObject]].mapAsyncUnordered(1) { bulk =>
          val strBulk = server match {
            case "server1" => bulk.flatMap(j => List(index1, j)).map(Json.stringify).mkString("", "\n", "\n")
            case "server2" => bulk.flatMap(j => List(index2, j)).map(Json.stringify).mkString("", "\n", "\n")
          }
          println(strBulk)
          wsClient.url("http://localhost:9200/_bulk")
            .withHttpHeaders("Content-Type" -> "application/x-ndjson")
            .post(strBulk)
        }
      })
      .map{ resp => resp.json.as[JsObject] }
      .map { resp =>
        println(resp)
        val errors = (resp \\ "items").map(i => (i \ "error").asOpt[JsObject].exists(_ => true))
        ImportStatus(nbSuccess = errors.count(e => !e), nbError = errors.count(identity))
      }
      .fold(ImportStatus()) { (s, e) => s.copy(nbSuccess = s.nbSuccess + e.nbSuccess, nbError = s.nbError + e.nbError) }
      .map(status => Json.stringify(Json.toJson(status)))
      .map(ByteString.apply)

    Ok.chunked(response).as("application/json")
  }

  def loadBalancing[In,Out](servers: List[String])(flow: String => Flow[In, Out, NotUsed]) =
    Flow.fromGraph { GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val parallelism = servers.size
      val merge = b.add(Merge[Out](parallelism))
      val balance = b.add(Balance[In](parallelism))

      for ((server, i) <- servers.zipWithIndex) {
        balance.out(i) ~> flow(server).async ~> merge.in(i)
      }

      FlowShape(balance.in, merge.out)
    }}

}
