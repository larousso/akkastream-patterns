package controllers;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.*;
import akka.util.ByteString;
import controllers.parsers.ByteStringBodyParser;
import io.vavr.collection.List;
import org.reactivecouchbase.json.JsObject;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import play.Logger;
import play.libs.ws.WSClient;
import play.mvc.BodyParser;
import play.mvc.Controller;
import play.mvc.Result;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.reactivecouchbase.json.Syntax.$;

@Singleton
public class CsvUploadController extends Controller {

    private final WSClient wsClient;
    private final ActorSystem system;

    @Inject
    public CsvUploadController(WSClient wsClient, ActorSystem system) {
        this.wsClient = wsClient;
        this.system = system;
    }

    //curl --no-buffer --limit-rate 1k http://localhost:9000/vikings
    public Result csvDownload() {

        Source<ByteString, NotUsed> csv = Source.fromCompletionStage(wsClient
                .url("http://localhost:9200/vikings/vikings/_search?scroll=30s")
                .addHeader("Content-Type", "application/json")
                .post(Json.stringify(Json.obj(
                            $("size", 5),
                            $("query", $("match_all", Json.obj()))
                    )))
            )
            .map(response -> Json.parse(response.getBody()))
            .flatMapConcat(jsonResp ->
                    Source
                        .single(jsonResp.field("hits").field("hits").asOptArray().toList().flatMap(List::ofAll).map(j -> j.field("_source").asObject()))
                        .concat(Source.unfoldAsync(jsonResp.field("_scroll_id").asString(), nextId -> {
                            Logger.info("New request "+System.currentTimeMillis());
                            return wsClient.url("http://localhost:9200/_search/scroll")
                                    .addHeader("Content-Type", "application/json")
                                    .post(Json.stringify(Json.obj(
                                            $("scroll", "30s"),
                                            $("scroll_id", nextId)
                                    )))
                                    .thenApply(resp -> {
                                        JsValue body = Json.parse(resp.getBody());
                                        List<JsObject> hits = List.ofAll(body.field("hits").field("hits").asArray()).map(j -> j.field("_source").asObject());
                                        if (hits.isEmpty()) {
                                            return Optional.empty();
                                        } else {
                                            return Optional.of(Pair.create(body.field("_scroll_id").asString(), hits));
                                        }
                                    });
                            }))
            )
            .mapConcat(l -> l)
            .map(json -> List.of(
                    json.field("name").asOptString().getOrElse(""),
                    json.field("place").asOptString().getOrElse("")
                ).mkString(";")
            )
            .intersperse("name;city\n", "\n", "\n")
            .map(ByteString::fromString)
            .watchTermination((nu, done) -> {
                done.whenComplete((__, e) -> {
                    if (e != null) {
                        Logger.error("Error during download", e);
                    }
                });
                return nu;
            });

        return ok().chunked(csv).as("text/csv");
    }

    //curl -X POST --data-binary @./conf/vikings.csv -H "Content-Type: text/csv" http://localhost:9000/vikings
    @BodyParser.Of(ByteStringBodyParser.class)
    public Result csvUpload() {

        Source<ByteString, ?> source = request().body().as(Source.class);
        JsObject indexMetadata = Json.obj($("index", Json.obj($("_index", "vikings"), $("_type", "vikings"))));

        Source<ByteString, ?> result = source
                .via(Framing.delimiter(ByteString.fromString("\n"), 10000, FramingTruncation.ALLOW))
                .map(ByteString::utf8String)
                .drop(1)
                .map(str -> str.split(";"))
                .map(fields -> Json.obj($("name", fields[0]), $("place", fields[1])))
                .grouped(20)
                .map(l -> List.ofAll(l).flatMap(e -> List.of(indexMetadata, e)).map(Json::stringify).mkString("", "\n", "\n"))
                .mapAsyncUnordered(4, bulk -> wsClient.url("http://localhost:9200/_bulk").addHeader("Content-Type", "application/x-ndjson").post(bulk))
                .map(resp -> Json.parse(resp.getBody()).asObject())
                .map(json -> {
                    List<Boolean> status = List.ofAll(json.field("items").asArray()).map(v -> v.fieldAsOpt("error").map(any -> true).getOrElse(false));
                    return ImportStatus.create(status.count(e -> !e), status.count(e -> e));
                })
                .fold(ImportStatus.empty(), (acc, elt) -> acc.incSuccess(elt.nbSuccess).incError(elt.nbErrors))
                .map(status -> Json.stringify(Json.obj($("nbSuccess", status.nbSuccess), $("nbErrors", status.nbErrors))))
                .map(ByteString::fromString);

        return ok().chunked(result).as("application/json");
    }

    //curl -XPOST http://localhost:9000/extras
    public Result extras() {

        Logger.info("Generating data in elasticsearch");

        JsObject indexMetadata = Json.obj($("index", Json.obj($("_index", "vikings"), $("_type", "vikings"))));
        Source<ByteString, ?> result = Source.range(0, 100000)
                .map(i -> Json.obj($("name", "viking-num"+i), $("place", "Kattegat")))
                .grouped(500)
                .map(l -> List.ofAll(l).flatMap(e -> List.of(indexMetadata, e)).map(Json::stringify).mkString("", "\n", "\n"))
                .mapAsyncUnordered(4, bulk ->
                        wsClient.url("http://localhost:9200/_bulk")
                                .addHeader("Content-Type", "application/x-ndjson")
                                .post(bulk)
                )
                .map(resp -> Json.parse(resp.getBody()).asObject())
                .alsoTo(Flow.<JsObject>create().grouped(20).to(Sink.foreach(e -> {
                    Logger.info("Next {} handled", 500 * 20);
                })))
                .map(json -> {
                    List<Boolean> status = List.ofAll(json.field("items").asArray()).map(v -> v.fieldAsOpt("error").map(any -> true).getOrElse(false));
                    int errors = status.count(e -> e);
                    if (errors > 0) {
                        Logger.error("Error during bulk");
                    }
                    return ImportStatus.create(status.count(e -> !e), errors);
                })
                .fold(ImportStatus.empty(), (acc, elt) -> acc.incSuccess(elt.nbSuccess).incError(elt.nbErrors))
                .map(status -> Json.stringify(Json.obj($("nbSuccess", status.nbSuccess), $("nbErrors", status.nbErrors))))
                .map(ByteString::fromString);

        return ok().chunked(result).as("application/json");
    }



    public static class ImportStatus {
        final Integer nbSuccess;
        final Integer nbErrors;

        public ImportStatus(Integer nbSuccess, Integer nbErrors) {
            this.nbSuccess = nbSuccess;
            this.nbErrors = nbErrors;
        }



        public static ImportStatus empty() {
            return new ImportStatus(0, 0);
        }
        public static ImportStatus create(Integer nbSuccess, Integer nbErrors) {
            return new ImportStatus(nbSuccess, nbErrors);
        }

        public ImportStatus incSuccess(Integer nbSuccess) {
            return new ImportStatus(this.nbSuccess + nbSuccess, nbErrors);
        }

        public ImportStatus incError(Integer nbErrors) {
            return new ImportStatus(nbSuccess, this.nbErrors + nbErrors);
        }

    }

}
