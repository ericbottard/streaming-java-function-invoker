package io.projectriff.invoker.server;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.function.Function;

import io.projectriff.invoker.NextHttpInputMessage;
import io.projectriff.invoker.NextHttpOutputMessage;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.springframework.http.HttpHeaders;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;

import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.core.ResolvableType;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.http.converter.ObjectToStringHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import reactor.core.publisher.GroupedFlux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * A reactive gRPC adapter that adapts a user function (with reactive signature) and makes
 * it invokable via the riff rpc protocol.
 *
 * <p>
 * This adapter reads the first signal, remembering the client's {@code Accept}'ed types,
 * then marshalls and un-marshalls input and output of the function, according to a set of
 * pre-defined {@link HttpMessageConverter} or injected ones if present in the application
 * context.
 * </p>
 *
 * @author Eric Bottard
 */
public class ReactorServerAdapter<T, V> extends ReactorRiffGrpc.RiffImplBase {

	private final Function<? super Flux<T>, ? extends Publisher<V>> fn;

	private final ResolvableType fnInputType;

	private final ResolvableType fnOutputType;

	private List<HttpMessageConverter> converters = new ArrayList<>();

	private MethodHandle mh;

	public ReactorServerAdapter(Function fn, FunctionInspector fi) {
		this.fn = fn;
		this.fnInputType = ResolvableType.forClass(fi.getInputType(fn));
		this.fnOutputType = ResolvableType.forClass(fi.getOutputType(fn));
		System.out.println("**** " + fi.getInputType(fn));
		System.out.println("**** " + fi.getOutputType(fn));
		System.out.println("**** " + fi.getInputWrapper(fn));
		System.out.println("**** " + fi.getOutputWrapper(fn));

		converters.add(new MappingJackson2HttpMessageConverter());
		converters.add(new FormHttpMessageConverter());
		StringHttpMessageConverter sc = new StringHttpMessageConverter();
		sc.setWriteAcceptCharset(false);
		converters.add(sc);
		ObjectToStringHttpMessageConverter oc = new ObjectToStringHttpMessageConverter(new DefaultConversionService());
		oc.setWriteAcceptCharset(false);
		converters.add(oc);

		MethodType applyType = MethodType.methodType(Object.class, Object.class);
		try {
			mh = MethodHandles.publicLookup().findVirtual(fn.getClass(), "apply", applyType);
		}
		catch (NoSuchMethodException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		mh = mh.bindTo(fn);
	}

	@Override
	public Flux<Signal> invoke(Flux<Signal> request) {
		return request
				.switchOnFirst((first, stream) -> {
					if (first.hasValue() && first.get().hasStart()) {
						List<MediaType> accept = MediaType.parseMediaTypes(first.get().getStart().getAccept());
						return stream.skip(1L)
								.doOnNext(System.out::println)
								.map(NextHttpInputMessage::new)
								.map(this::decode)
								.transform(t())
								.doOnError(e -> System.out.println("Seen it: " + e))
								.map(encode(accept))
								.map(NextHttpOutputMessage::asSignal)
								.doOnError(Throwable::printStackTrace);
					}
					return Flux.error(new RuntimeException("Expected first frame to be of type Start"));
				});
	}










	@SuppressWarnings("unchecked")
	private Function<Flux<Tuple2<Object, Integer>>, Publisher<Tuple2<Object, Integer>>> t() {
		Tuple2<Object, Integer>[] startTuples = new Tuple2[mh.type().parameterCount()];
		for (int i = 0; i < startTuples.length; i++) {
			startTuples[i] = Tuples.of(new Object(), i);
		}

		return f -> f.startWith(Flux.fromArray(startTuples))
				.groupBy(Tuple2::getT2, Tuple2::getT1)
				.collectSortedList(Comparator.comparingInt(GroupedFlux::key))
				.flatMapMany(groups -> {
					try {
						return Flux.merge((Flux[]) mh.invoke(groups.stream().map(g -> g.skip(1)).toArray(Object[]::new)));
					} catch (Throwable t) {
						throw Exceptions.propagate(t);
					}
				});
	}

	private Function<Tuple2<Object, Integer>, NextHttpOutputMessage> encode(List<MediaType> accept) {
		return t -> {
			NextHttpOutputMessage out = new NextHttpOutputMessage();
			out.getHeaders().set("RiffOutput", t.getT2().toString());
			Object o = t.getT1();
			for (MediaType accepted : accept) {
				for (HttpMessageConverter converter : converters) {
					for (Object mt : converter.getSupportedMediaTypes()) {
						MediaType mediaType = (MediaType) mt;
						if (o != null && accepted.includes(mediaType) && converter.canWrite(o.getClass(), mediaType)) {
							try {
								converter.write(o, mediaType, out);
								return out;
							}
							catch (IOException e) {
								throw new HttpMessageNotWritableException("could not write message", e);
							}
						}
						else if (o == null && accepted.includes(mediaType)
								&& converter.canWrite(fnOutputType.toClass(), mediaType)) {
							try {
								converter.write(o, mediaType, out);
								return out;
							}
							catch (IOException e) {
								throw new HttpMessageNotWritableException("could not write message", e);
							}
						}
					}
				}
			}
			throw new HttpMessageNotWritableException(
					String.format("could not find converter for accept = '%s' and return value of type %s", accept,
							o == null ? "[null]" : o.getClass()));
		};
	}

	private Tuple2<Object, Integer> decode(HttpInputMessage m) {
		MediaType contentType = m.getHeaders().getContentType();
		Integer riffInput = Integer.valueOf(m.getHeaders().getFirst("RiffInput"));

		for (HttpMessageConverter converter : converters) {
			if (converter.canRead(fnInputType.toClass(), contentType)) {
				try {
                    return Tuples.of((T) converter.read(fnInputType.toClass(), m), riffInput);
				}
				catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
		throw new HttpMessageNotReadableException("No suitable converter", m);
	}

}

			/*
			DirectProcessor[] processors = new DirectProcessor[mh.type().parameterCount()];
			BaseSubscriber<Tuple2<Object, Integer>> inSub = new BaseSubscriber<>() {
				@Override
				protected void hookOnSubscribe(Subscription subscription) {
				}

				@Override
				protected void hookOnNext(Tuple2<Object, Integer> value) {
					processors[value.getT2()].onNext(value.getT1());
				}

				@Override
				protected void hookOnComplete() {
					for(DirectProcessor p : processors) {
						p.onComplete();
					}
				}

				@Override
				protected void hookOnError(Throwable throwable) {
					for(DirectProcessor p : processors) {
						p.onError(throwable);
					}
				}
			};

			for (int i = 0; i < processors.length; i++) {
				processors[i] = DirectProcessor.create();
			}

			Flux[] fluxes = new Flux[processors.length];

			for (int i = 0; i < processors.length; i++) {
				fluxes[i] = processors[i].onBackpressureBuffer()
						.doOnRequest(inSub::request);
			}

			in.subscribe(inSub);

			try {
				Flux[] results = (Flux[]) mh.invoke(fluxes);
				Flux merged = Flux.merge(results);
				return (Publisher<Object>) mh.invoke(fluxes);
			}
			catch (Throwable e) {
				throw new RuntimeException(e);
			}

			 */
