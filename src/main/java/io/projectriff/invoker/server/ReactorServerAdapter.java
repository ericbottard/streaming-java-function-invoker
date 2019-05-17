package io.projectriff.invoker.server;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import io.projectriff.invoker.NextHttpInputMessage;
import io.projectriff.invoker.NextHttpOutputMessage;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

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

	public static final ResolvableType FLUX_TYPE = ResolvableType.forClassWithGenerics(Flux.class, Object.class);

	private List<HttpMessageConverter> converters = new ArrayList<>();

	private MethodHandle mh;

    private ResolvableType outputType;

    private Class<?>[] inputTypes;


	public ReactorServerAdapter(Object function, Method m, FunctionInspector fi) throws IllegalAccessException {
		MethodHandle mh = MethodHandles.publicLookup().unreflect(m);
		mh = mh.bindTo(function);
		this.mh = mh;

		outputType = ResolvableType.forMethodReturnType(m);
		inputTypes = new Class[m.getParameterCount()];
		for (int i = 0; i < m.getParameterCount(); i++) {
			ResolvableType type = ResolvableType.forMethodParameter(m, i);
			// if (!type.isAssignableFrom(FLUX_TYPE)) {
			// throw new RuntimeException("Expected parameter of type Flux at position " + i + ": " +
			// m);
			// }
			inputTypes[i] = type.resolveGeneric(0);
		}

		converters.add(new MappingJackson2HttpMessageConverter());
		converters.add(new FormHttpMessageConverter());
		StringHttpMessageConverter sc = new StringHttpMessageConverter();
		sc.setWriteAcceptCharset(false);
		converters.add(sc);
		ObjectToStringHttpMessageConverter oc = new ObjectToStringHttpMessageConverter(new DefaultConversionService());
		oc.setWriteAcceptCharset(false);
		converters.add(oc);

	}

	@Override
	public Flux<Signal> invoke(Flux<Signal> request) {
		return request
				.switchOnFirst((first, stream) -> {
					if (!first.hasValue()) {
						return Flux.error(new RuntimeException("Expected first frame to be of type Start"));
					}
					Signal firstSignal = first.get();
					if (!firstSignal.hasStart()) {
						return Flux.error(new RuntimeException("Expected first frame to be of type Start"));
					}

					Tuple2<Object, Integer>[] startTuples = new Tuple2[mh.type().parameterCount()];
					for (int i = 0; i < startTuples.length; i++) {
						startTuples[i] = Tuples.of(new Object(), i);
					}

					List<MediaType> accept = MediaType.parseMediaTypes(firstSignal.getStart().getAccept());
					return stream
							.doOnNext(m -> System.err.println("STEP1 " + m))
							.skip(1L)
							.doOnNext(m -> System.err.println("STEP1.5 " + m))
							.map(NextHttpInputMessage::new)
							.doOnNext(m -> System.err.println("STEP2 " + m))
							.map(this::decode)
							.doOnNext(m -> System.err.println("STEP3 " + m))
							.transform(t())
							.doOnNext(m -> System.err.println("STEP4 " + m))
							.map(encode(accept))
							.doOnNext(m -> System.err.println("STEP5 " + m))
							.map(NextHttpOutputMessage::asSignal)
							.doOnNext(m -> System.err.println("STEP6 " + m))
							.doOnNext(m -> System.err.println("STEP7 " + m))
							.doOnError(Throwable::printStackTrace);
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
				.take(startTuples.length)
				.collectSortedList(Comparator.comparingInt(GroupedFlux::key))
				.flatMapMany(groups -> {
					try {
						Object[] args = groups.stream().map(g -> g.skip(1)).toArray(Object[]::new);
						Flux<Object>[] bareOutputs = (Flux<Object>[]) mh.invokeWithArguments(args);
						Flux<Tuple2<Object, Integer>>[] withOutputIndices =new Flux[bareOutputs.length];
						for (int i = 0; i < bareOutputs.length; i++) {
							int j = i;
							withOutputIndices[i] = bareOutputs[i].map(o -> Tuples.of(o, j));
						}
						return Flux.merge(withOutputIndices);
					} catch (Throwable t) {
						throw Exceptions.propagate(t);
					}
				});


		/*

		DirectProcessor[] processors = new DirectProcessor[mh.type().parameterCount()];
		BaseSubscriber<Tuple2<Object, Integer>> inSub = new BaseSubscriber<>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				System.out.println(subscription);
				super.hookOnSubscribe(subscription);
			}

			@Override
			protected void hookOnNext(Tuple2<Object, Integer> value) {
				processors[value.getT2()].onNext(value.getT1());
			}

			@Override
			protected void hookOnComplete() {
				for (DirectProcessor p : processors) {
					p.onComplete();
				}
			}

			@Override
			protected void hookOnError(Throwable throwable) {
				for (DirectProcessor p : processors) {
					p.onError(throwable);
				}
			}
		};

		for (int i = 0; i < processors.length; i++) {
			processors[i] = DirectProcessor.create();
		}

		Flux[] fluxes = new Flux[processors.length];

		for (int i = 0; i < processors.length; i++) {
			fluxes[i] = processors[i]
					.onBackpressureBuffer().doOnRequest(inSub::request);
		}

		return f -> {
			f.subscribe(inSub);
			try {
				Object[] os = fluxes;
                Object result = mh.invokeWithArguments(Arrays.asList(os));
                Flux[] results;
                if (outputType.isArray()) {
                    results = (Flux[]) result;
                } else {
                    results = new Flux[] {(Flux<?>) result};
                }
                for (int i = 0; i < results.length; i++) {
					var ii = i;
					results[i] = results[i].map(o -> Tuples.of(o, ii));
				}
				return Flux.merge(results);
			}
			catch (Throwable e) {
				throw new RuntimeException(e);
			}

		};

		*/
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
						if (accepted.includes(mediaType) && converter.canWrite(o.getClass(), mediaType)) {
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
							o.getClass()));
		};
	}

	private Tuple2<Object, Integer> decode(HttpInputMessage m) {
		MediaType contentType = m.getHeaders().getContentType();
		Integer riffInput = Integer.valueOf(m.getHeaders().getFirst("RiffInput"));

		var type = inputTypes[riffInput];

		for (HttpMessageConverter converter : converters) {
			if (converter.canRead(type, contentType)) {
				try {
					return Tuples.of((T) converter.read(type, m), riffInput);
				}
				catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
		throw new HttpMessageNotReadableException("No suitable converter", m);
	}

}
