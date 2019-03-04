package io.projectriff.invoker.streaming;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.google.protobuf.ByteString;
import reactor.core.publisher.Flux;

import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.core.ResolvableType;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.http.converter.ObjectToStringHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

/**
 */
public class ReactorServerThirdAdapter extends ReactorRiffGrpc.RiffImplBase {

	private final Function fn;

	private final ResolvableType fnInputType;

	private final ResolvableType fnOutputType;

	private List<HttpMessageConverter> converters = new ArrayList<>();

	public ReactorServerThirdAdapter(Function fn, FunctionInspector fi) {
		this.fn = fn;
		this.fnInputType = ResolvableType.forClass(fi.getInputType(fn));
		this.fnOutputType = ResolvableType.forClass(fi.getOutputType(fn));

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
					if (first.hasValue() && first.get().hasStart()) {
						MediaType accept = MediaType.valueOf(first.get().getStart().getAccept());
						Flux<?> transform = stream.skip(1L)
								.doOnNext(s -> {
									System.out.println(s);
								})
								.map(ReactorServerThirdAdapter::toHttpMessage)
								.map(this::decode)
								.transform(fn);
						return transform
								.map(encode(accept))
								.map(NextHttpOutputMessage::asSignal);
					}
					return Flux.error(new RuntimeException("Expected first frame to be of type Start"));
				});
	}

	private Function<Object, NextHttpOutputMessage> encode(MediaType accept) {
		return o -> {
			NextHttpOutputMessage out = new NextHttpOutputMessage();
			for (HttpMessageConverter converter : converters) {
				for (Object mt : converter.getSupportedMediaTypes()) {
					MediaType mediaType = (MediaType) mt;
					if (o != null && accept.includes(mediaType) && converter.canWrite(o.getClass(), mediaType)) {
						try {
							converter.write(o, mediaType, out);
							return out;
						}
						catch (IOException e) {
							throw new HttpMessageNotWritableException("could not write message", e);
						}
					}
					else if (o == null && accept.includes(mediaType)
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
			throw new HttpMessageNotWritableException(
					String.format("could not find converter for accept = '%s' and return value of type %s", accept,
							o == null ? "[null]" : o.getClass()));
		};
	}

	private static HttpInputMessage toHttpMessage(Signal s) {
		return new HttpInputMessage() {
			@Override
			public InputStream getBody() throws IOException {
				return s.getNext().getPayload().newInput();
			}

			@Override
			public HttpHeaders getHeaders() {
				HttpHeaders headers = new HttpHeaders();
				s.getNext().getHeadersMap().entrySet()
						.forEach(e -> headers.add(e.getKey(), e.getValue()));
				return headers;
			}
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

	private static class NextHttpOutputMessage implements HttpOutputMessage {

		private final ByteString.Output output = ByteString.newOutput();

		private final HttpHeaders headers = new HttpHeaders();

		@Override
		public OutputStream getBody() throws IOException {
			return output;
		}

		@Override
		public HttpHeaders getHeaders() {
			return headers;
		}

		public Signal asSignal() {
			return Signal.newBuilder().setNext(
					Next.newBuilder()
							.setPayload(output.toByteString())
							.putAllHeaders(headers.toSingleValueMap()))
					.build();
		}

	}
}
