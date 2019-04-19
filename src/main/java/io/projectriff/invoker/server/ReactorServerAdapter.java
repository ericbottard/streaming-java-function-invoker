package io.projectriff.invoker.server;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.function.Function;

import io.projectriff.invoker.NextHttpInputMessage;
import io.projectriff.invoker.NextHttpOutputMessage;
import org.reactivestreams.Publisher;
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
public class ReactorServerAdapter extends ReactorRiffGrpc.RiffImplBase {

	private final Function fn;

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
						Flux<?> transform = stream.skip(1L)
								.doOnNext(s -> {
									System.out.println(s);
								})
								.map(NextHttpInputMessage::new)
								.map(this::decode)
								.transform(t())
								.doOnError(e -> System.out.println("Seen it: " + e));
						return transform
								.map(encode(accept))
								.map(NextHttpOutputMessage::asSignal)
								.doOnError(Throwable::printStackTrace);
					}
					return Flux.error(new RuntimeException("Expected first frame to be of type Start"));
				});
	}

	private Function t() {
		return f -> {
			try {
				return mh.invoke(f);
			}
			catch (Throwable e) {
				throw new RuntimeException(e);
			}
		};
	}

	private Function<Object, NextHttpOutputMessage> encode(List<MediaType> accept) {
		return o -> {
			NextHttpOutputMessage out = new NextHttpOutputMessage();
			out.getHeaders().set("RiffOutput", "0"); // Hardcode the fact that there is only one output for now
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

	private Object decode(HttpInputMessage m) {
		MediaType contentType = m.getHeaders().getContentType();
		for (HttpMessageConverter converter : converters) {
			if (converter.canRead(fnInputType.toClass(), contentType)) {
				try {
					return converter.read(fnInputType.toClass(), m);
				}
				catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
		throw new HttpMessageNotReadableException("No suitable converter", m);
	}

}
