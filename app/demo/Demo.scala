package demo

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.http.scaladsl.util.FastFuture
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout
import controllers.Graph
import demo.Demo.StoreActor.GetAll

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object Demo extends App {

  implicit val system = ActorSystem()

  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  case class PartialUser(id: Option[String] = None, name: Option[String] = None, age: Option[Int] = None)
  case class User(id: String, name: String, age: Int) {
    def merge(partialUser: PartialUser) : User = partialUser match {
      case PartialUser(_, Some(n), Some(a)) => this.copy(name = n, age = a)
      case PartialUser(_, Some(n), None) => this.copy(name = n)
      case PartialUser(_, None, Some(a)) => this.copy(age = a)
      case _ => this
    }
  }


  val store = Store("store1")

  val insert = Source(0 to 10) map (id => (s"$id", User(s"$id", s"name$id", id)))
  val nameUpdate = Source(0 to 10) map (id => (s"$id",PartialUser(name = Some(s"name${id * id}"))))
  val ageUpdate = Source(0 to 10) map (id => (s"$id",PartialUser(age = Some(id * id))))

  insert
    .mapAsync(2) {
      case (id, user) => store.set(id, user)
    }
    .fold(0) { (acc, u) => acc + 1 }
    .flatMapConcat(_ =>
      nameUpdate.interleave(ageUpdate, 1).mapAsync(4) { case (id, p) => store.updateUser(id, p) }
    )
    .runWith(Sink.ignore)
    .flatMap(_ => store.getAll())
    .onComplete {
      case Success(allUsers) => println(s"1 : \n${allUsers.mkString("\n")}")
      case Failure(e) => e.printStackTrace()
    }

  val store2 = Store("store2")

  val update = Flow[(String, PartialUser)].flatMapConcat {
    case (id, p) => Source.fromFuture(store2.updateUser(id, p))
  }

  insert
    .mapAsync(2) {
      case (id, user) => store2.set(id, user)
    }
    .fold(0) { (acc, u) => acc + 1 }
    .flatMapConcat(_ =>
      nameUpdate.interleave(ageUpdate, 1) via Graph.sharding(4, update)
    )
    .runWith(Sink.ignore)
    .flatMap(_ => store2.getAll())
    .onComplete {
      case Success(allUsers) => println(s"2 : \n${allUsers.mkString("\n")}")
      case Failure(e) => e.printStackTrace()
    }



  case class Store(name: String)(implicit system: ActorSystem) {
    import akka.pattern._
    import system.dispatcher
    implicit val timeout = Timeout(1.second)
    private val storeActor = system.actorOf(Props[StoreActor], s"store-$name")
    def getById(id: String): Future[Option[User]] = (storeActor ? StoreActor.GetById(id)).mapTo[Option[User]]
    def set(id: String, user: User): Future[User] = (storeActor ? StoreActor.Set(id, user)).mapTo[User]
    def getAll() : Future[Seq[User]] = (storeActor ? StoreActor.GetAll).mapTo[Seq[User]]

    def updateUser(id: String, partialUser: PartialUser) : Future[User] = {
      getById(id) flatMap {
        case Some(user) => set(id, user.merge(partialUser))
        case None => FastFuture.failed(new RuntimeException("Unknown user"))
      }
    }
  }

  object StoreActor {
    sealed trait Msg
    case class GetById(id: String) extends Msg
    case class Set(id: String, user: User) extends Msg
    case object GetAll extends Msg
  }
  class StoreActor extends Actor with ActorLogging {
    private var datas = Map.empty[String, User]
    override def receive: Receive = {
      case StoreActor.GetById(id) =>
        //log.info(s"Get user with id $id")
        sender() ! datas.get(id)
      case StoreActor.Set(id, user) =>
        //log.info(s"set user $user with id $id")
        datas = datas + (id -> user)
        sender() ! user
      case GetAll =>
        sender() ! datas.values.toSeq
    }
  }

}
