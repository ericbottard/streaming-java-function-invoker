package com.acme;

import io.projectriff.invoker.client.OtherClient;
import io.projectriff.invoker.server.Signal;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.time.Duration;
import java.util.function.BiFunction;

public class Zipper implements BiFunction<Flux<String>, Flux<Integer>, Flux<String>[]> {

    private static final String[] numbers = new String[]{"zero", "one", "two", "three", "four", "five"};

    @Override
    public Flux<String>[] apply(Flux<String> stringFlux, Flux<Integer> integerFlux) {
        return new Flux[]{stringFlux.zipWith(integerFlux).flatMap(t -> {
            if (t.getT2().intValue() == 0) {
                return Flux.empty();
            }
            return Flux.just(t.getT1()).repeat(t.getT2() - 1);
        })};
    }

    public static void main(String[] args) throws IOException {
        Flux<String> strings = Flux.interval(Duration.ofMillis(5000L)).map(i -> numbers[i.intValue() % numbers.length]);
        Flux<Integer> ints = Flux.interval(Duration.ofMillis(6000L)).map(i -> i.intValue() % numbers.length);
        new Zipper().apply(
                strings,
                ints
        )[0].subscribe(System.out::println);
        System.in.read();
    }

}
