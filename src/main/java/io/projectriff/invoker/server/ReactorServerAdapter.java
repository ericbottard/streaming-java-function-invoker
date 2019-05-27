package io.projectriff.invoker.server;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.protobuf.ProtocolStringList;
import io.projectriff.invoker.NextHttpInputMessage;
import io.projectriff.invoker.NextHttpOutputMessage;
import io.projectriff.invoker.rpc.InputSignal;
import io.projectriff.invoker.rpc.OutputSignal;
import io.projectriff.invoker.rpc.ReactorRiffGrpc;
import org.reactivestreams.Publisher;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.*;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

/**
 * A reactive gRPC adapter that adapts a user function (with reactive signature) and makes
 * it invokable via the riff rpc protocol.
 *
 * <p>
 * This adapter reads the first signal, remembering the client's {@code expectedContentTypes},
 * then marshalls and un-marshalls input and output of the function, according to a set of
 * pre-defined {@link HttpMessageConverter} or injected ones if present in the application
 * context (TODO).
 * </p>
 *
 * @author Eric Bottard
 * @author Florent Biville
 */
public class ReactorServerAdapter<T, V> extends ReactorRiffGrpc.RiffImplBase {

	private List<HttpMessageConverter> converters = new ArrayList<>();

	private MethodHandle mh;

    private Class<?>[] inputTypes;


	public ReactorServerAdapter(Object function, Method m, Class[] types) throws IllegalAccessException {
		MethodHandle mh = MethodHandles.publicLookup().unreflect(m);
		mh = mh.bindTo(function);
		this.mh = mh;

		inputTypes = types;

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
	public Flux<OutputSignal> invoke(Flux<InputSignal> request) {
		return request
				.switchOnFirst((first, stream) -> {
					if (!first.hasValue()) {
						return Flux.error(new RuntimeException("Expected first frame to be of type Start"));
					}
					InputSignal firstSignal = first.get();
					if (!firstSignal.hasStart()) {
						return Flux.error(new RuntimeException("Expected first frame to be of type Start"));
					}

					Tuple2<Object, Integer>[] startTuples = new Tuple2[mh.type().parameterCount()];
					for (int i = 0; i < startTuples.length; i++) {
						startTuples[i] = Tuples.of(new Object(), i);
					}

					ProtocolStringList expectedContentTypesList = firstSignal.getStart().getExpectedContentTypesList();
					List<List<MediaType>> accept = expectedContentTypesList.stream().map(MediaType::parseMediaTypes).collect(Collectors.toList());
					return stream
							.skip(1L)
							.map(NextHttpInputMessage::new)
							.map(this::decode)
							.transform(t())
							.map(encode(accept))
							.map(NextHttpOutputMessage::asSignal)
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
						Object result =  mh.invokeWithArguments(args);
						Flux<?>[] bareOutputs = new Flux<?>[1];
						if (result.getClass().isArray()) {
							bareOutputs = (Flux<?>[]) result;
						} else {
							bareOutputs[0] = (Flux<?>) result;
						}
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
	}

	private Function<Tuple2<Object, Integer>, NextHttpOutputMessage> encode(List<List<MediaType>> expectedContentTypesList) {
		return t -> {
			Integer index = t.getT2();
			Object o = t.getT1();
			NextHttpOutputMessage out = new NextHttpOutputMessage();
			out.getHeaders().set("RiffOutput", index.toString());
			List<MediaType> expectedContentTypes = expectedContentTypesList.get(index);
			for (MediaType accepted : expectedContentTypes) {
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
					String.format("could not find converter for accept = '%s' and return value of type %s", expectedContentTypesList,
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
