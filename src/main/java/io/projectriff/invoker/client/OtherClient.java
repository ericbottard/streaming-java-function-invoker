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

    public static void main(String[] args) throws IOException {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080)
                .usePlaintext()
                .overrideAuthority("encode.default.example.com")
                .build();

        var stub = ReactorRiffGrpc.newReactorStub(channel);

        Signal start = Signal.newBuilder().setStart(Start.newBuilder().setAccept("application/json").build()).build();

        Flux<Signal> request = Flux.concat(
                Flux.just(start),
                Flux.interval(Duration.ofMillis(500L)).map(i -> i / 5).map(OtherClient::toSignal)
        );
        Flux<Signal> response = stub.invoke(request.log("HELLO "));

        response.subscribe(System.out::println);
        System.in.read();


    }

    private static Signal toSignal(Long l) {
        return Signal.newBuilder()
                .setNext(Next.newBuilder()
                        .setPayload(ByteString.copyFromUtf8("" + l))
                        .putHeaders("Content-Type", "text/plain")
                        .putHeaders("RiffInput", "0")
                )
            .build();
    }
}
