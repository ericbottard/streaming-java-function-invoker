package io.projectriff.invoker.client;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.projectriff.invoker.server.Next;
import io.projectriff.invoker.server.ReactorRiffGrpc;
import io.projectriff.invoker.server.Signal;
import io.projectriff.invoker.server.Start;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.time.Duration;

public class OtherClient {

    private static final String[] numbers = new String[]{"zero", "one", "two", "three", "four", "five"};

    public static void main(String[] args) throws IOException {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080)
                .usePlaintext()
                .overrideAuthority("encode.default.example.com")
                .build();

        var stub = ReactorRiffGrpc.newReactorStub(channel);

        Signal start = Signal.newBuilder().setStart(Start.newBuilder().setAccept("application/json").build()).build();

        Flux<Signal> strings = Flux.interval(Duration.ofMillis(5000L)).map(i -> i % numbers.length).map(OtherClient::toSignalString);
        Flux<Signal> ints = Flux.interval(Duration.ofMillis(6000L)).map(i -> i % numbers.length).map(OtherClient::toSignalInt);

        Flux<Signal> request = Flux.concat(
                Flux.just(start),
                strings.mergeWith(ints)
        );
        Flux<Signal> response = stub.invoke(request.doOnNext(System.err::println));

        response.subscribe(s -> System.out.println(s.getNext().getPayload().toStringUtf8()));
        System.in.read();


    }

    private static Signal toSignalString(Long l) {
        return Signal.newBuilder()
                .setNext(Next.newBuilder()
                        .setPayload(ByteString.copyFromUtf8(numbers[l.intValue()]))
                        .putHeaders("Content-Type", "text/plain")
                        .putHeaders("RiffInput", "0")
                )
            .build();
    }

    private static Signal toSignalInt(Long l) {
        return Signal.newBuilder()
                .setNext(Next.newBuilder()
                        .setPayload(ByteString.copyFromUtf8("" + l))
                        .putHeaders("Content-Type", "text/plain")
                        .putHeaders("RiffInput", "1")
                )
            .build();
    }
}
