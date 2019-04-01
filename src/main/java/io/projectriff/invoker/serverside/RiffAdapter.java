package io.projectriff.invoker.serverside;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import io.projectriff.invoker.NextHttpInputMessage;
import io.projectriff.invoker.NextHttpOutputMessage;
import io.projectriff.invoker.server.Message;
import io.projectriff.invoker.server.Riff;
import io.rsocket.rpc.frames.Metadata;
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

public class RiffAdapter implements Riff {

	private final Function fn;

	private final ResolvableType fnInputType;

	private final ResolvableType fnOutputType;

	private List<HttpMessageConverter> converters = new ArrayList<>();

	public RiffAdapter(Function fn, FunctionInspector fi) {
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
	}

	@Override
	public Flux<Message> invoke(Publisher<Message> messages, ByteBuf metadata) {
		ByteBuf userMetadata = Metadata.getMetadata(metadata);
		List<MediaType> accept = MediaType.parseMediaTypes(userMetadata.toString(Charset.defaultCharset()));
		Flux<?> transform = Flux.from(messages)
				.doOnNext(s -> {
					System.out.println(s);
				})
				.map(NextHttpInputMessage::new)
				.map(this::decode)
				.transform(fn)
				.doOnError(e -> System.out.println("Seen it: " + e));
		return transform
				.map(encode(accept))
				.map(NextHttpOutputMessage::asMessage)
				.doOnError(Throwable::printStackTrace);

	}

	private Function<Object, NextHttpOutputMessage> encode(List<MediaType> accept) {
		return o -> {
			NextHttpOutputMessage out = new NextHttpOutputMessage();
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
