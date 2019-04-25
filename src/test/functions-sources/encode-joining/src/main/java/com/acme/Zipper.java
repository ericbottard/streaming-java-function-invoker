package com.acme;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class Zipper implements BiFunction<Flux<String>, Flux<Integer>, Flux<String>[]> {

	@Override
	public Flux<String>[] apply(Flux<String> stringFlux, Flux<Integer> integerFlux) {
		return new Flux[] {stringFlux.zipWith(integerFlux).flatMap(t -> Flux.just(t.getT1()).repeat(t.getT2()))};
	}

}
