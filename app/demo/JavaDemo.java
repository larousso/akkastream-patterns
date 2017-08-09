package demo;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
import io.vavr.collection.List;
import scala.concurrent.duration.FiniteDuration;

import static java.util.concurrent.TimeUnit.SECONDS;

public class JavaDemo {

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("AkkaStreams");
        Materializer materializer = ActorMaterializer.create(system);

        Source<String, NotUsed> hey = Source.repeat("Hey");
        Source<String, Cancellable> yo = Source.tick(FiniteDuration.Zero(), FiniteDuration.create(1, SECONDS), "Yo");

        Source.zipWithN(l -> List.ofAll(l).mkString(" "), List.of(hey, yo).toJavaList())
                .scan("", (acc, elt) -> acc + " - " + elt)
                .runForeach(e -> System.out.println(e), materializer);

    }
}
