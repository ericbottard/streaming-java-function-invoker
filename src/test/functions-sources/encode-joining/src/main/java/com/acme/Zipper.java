package com.acme;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class Zipper implements BiFunction<Flux<String>, Flux<Integer>, Flux<String>> {

	public Flux<?>[] apply(Flux<String> input) {
		return new Flux<?>[] {input.
			compose(Zipper::detectRun).
			flatMap(buf -> Flux.just(buf.size(), buf.get(0)))};
	}
	
	private static Publisher<List<String>> detectRun(Flux<String> s) {
		return s.bufferUntil(new Predicate<String>() {
			String old;

			@Override
			public boolean test(String item) {
				try {
					return !item.equals(old);
				} finally {
					old = item;
				}
			}
		}, true);  // cut before item that terminates the buffer
	}

	@Override
	public Flux<String> apply(Flux<String> stringFlux, Flux<Integer> integerFlux) {
		return stringFlux.zipWith(integerFlux).flatMap(t2 -> Flux.just(t2.getT1()).repeat(t2.getT2()-1));
	}
}
