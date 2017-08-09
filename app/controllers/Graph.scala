package controllers

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.FlowShape
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Partition, Zip}

import scala.concurrent.ExecutionContext
import scala.util.hashing.MurmurHash3

object Graph {

  val system = ActorSystem()

  implicit val ec: ExecutionContext = system.dispatchers.lookup("my-app.blocking-io-dispatcher")

  def count[In, Out](aFlow: Flow[In, Out, NotUsed]): Flow[In, (Out, Int), NotUsed] =
    Flow.fromGraph { GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val bcast = b.add(Broadcast[In](2))
      val zip = b.add(Zip[Out, Int]())

      bcast ~> Flow[In].fold(0) { (acc, _) => acc + 1 } ~> zip.in1
      bcast ~> aFlow ~> zip.in0

      FlowShape(bcast.in, zip.out)
    }
  }

  def sharding[In, Out](parallelism: Int, aFlow: Flow[(String, In), Out, NotUsed]): Flow[(String, In), Out, NotUsed] =
    Flow.fromGraph { GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val merge = b.add(Merge[Out](parallelism))
      val partition = b.add(Partition[(String, In)](parallelism, {
        case (id, _) => Math.abs(MurmurHash3.stringHash(id) % parallelism)
      }))

      for (i <- 0 until parallelism) {
        partition.out(i) ~> aFlow.async ~> merge.in(i)
      }

      FlowShape(partition.in, merge.out)
    }
  }

}
