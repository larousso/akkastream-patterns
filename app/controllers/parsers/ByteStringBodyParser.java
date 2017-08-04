package controllers.parsers;

import akka.stream.javadsl.Source;
import akka.util.ByteString;
import play.libs.F;
import play.libs.streams.Accumulator;
import play.mvc.BodyParser;
import play.mvc.Http;
import play.mvc.Result;

import javax.inject.Inject;
import java.util.concurrent.Executor;

public class ByteStringBodyParser implements BodyParser<Source<ByteString, ?>> {

    private Executor executor;

    @Inject
    public ByteStringBodyParser(Executor executor) {
        this.executor = executor;
    }

    @Override
    public Accumulator<ByteString, F.Either<Result, Source<ByteString, ?>>> apply(Http.RequestHeader request) {
        Accumulator<ByteString, Source<ByteString, ?>>  source = Accumulator.source();
        return source.map(F.Either::Right, executor);
    }
}