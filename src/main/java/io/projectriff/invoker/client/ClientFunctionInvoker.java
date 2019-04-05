package io.projectriff.invoker.client;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.projectriff.invoker.NextHttpInputMessage;
import io.projectriff.invoker.NextHttpOutputMessage;
import io.projectriff.invoker.server.Message;
import io.projectriff.invoker.server.RiffClient;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import reactor.core.publisher.Flux;

import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.http.MediaType;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.http.converter.ObjectToStringHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

/**
 * A helper for invoking riff functions from the client side.
 *
 * <p>
 * Takes care of calling the rsocket client side with the appropriate, marshalling the
 * input and un-marshalling the invocation result.
 * </p>
 *
 * @author Eric Bottard
 */
public class ClientFunctionInvoker<T, R> implements Function<Flux<T>, Flux<R>> {

	private final RiffClient stub;

	private final List<HttpMessageConverter> converters = new ArrayList<>();

	private String accept;

	private final Class<T> inputType;

	private final Class<R> outputType;

	public ClientFunctionInvoker(RSocket rSocket, Class<T> inputType, Class<R> outputType) {
		stub = new RiffClient(rSocket);
		this.inputType = inputType;
		this.outputType = outputType;

		converters.add(new MappingJackson2HttpMessageConverter());
		converters.add(new FormHttpMessageConverter());
		StringHttpMessageConverter sc = new StringHttpMessageConverter();
		sc.setWriteAcceptCharset(false);
		converters.add(sc);
		ObjectToStringHttpMessageConverter oc = new ObjectToStringHttpMessageConverter(new DefaultConversionService());
		oc.setWriteAcceptCharset(false);
		converters.add(oc);

		computeAccept();

	}

	private void computeAccept() {
		List<MediaType> allSupportedMediaTypes = converters.stream()
				.filter(converter -> converter.canRead(this.outputType, null))
				.flatMap(this::getSupportedMediaTypes)
				.distinct()
				.sorted(MediaType.SPECIFICITY_COMPARATOR)
				.collect(Collectors.toList());
		accept = MediaType.toString(allSupportedMediaTypes);
	}

	private Stream<MediaType> getSupportedMediaTypes(HttpMessageConverter converter) {
		List<MediaType> supportedMediaTypes = converter.getSupportedMediaTypes();
		// This drops charsets
		return supportedMediaTypes.stream().map(mt -> new MediaType(mt.getType(), mt.getSubtype()));
	}

	@Override
	public Flux<R> apply(Flux<T> input) {
		Flux<Message> request = input.map(this::convertToSignal);
		ByteBuf metadata = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, accept);
		Flux<Message> response = stub.invoke(request, metadata);
		return response.map(this::convertFromSignal);
	}

	private R convertFromSignal(Message signal) {
		String ct = signal.getHeadersOrThrow("Content-Type");
		MediaType contentType = MediaType.parseMediaType(ct);
		NextHttpInputMessage inputMessage = new NextHttpInputMessage(signal);
		try {
			for (HttpMessageConverter converter : converters) {
				if (converter.canRead(outputType, contentType)) {
					return (R) converter.read(outputType, inputMessage);
				}
			}
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
		throw new HttpMessageNotReadableException("Could not find suitable converter", inputMessage);
	}

	private Message convertToSignal(T t) {
		if (t != null) {
			try {
				for (HttpMessageConverter converter : converters) {
					for (Object mediaType : converter.getSupportedMediaTypes()) {
						if (converter.canWrite(t.getClass(), (MediaType) mediaType)) {
							NextHttpOutputMessage outputMessage = new NextHttpOutputMessage();
							converter.write(t, (MediaType) mediaType, outputMessage);
							return outputMessage.asMessage();
						}
					}
				}
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
			throw new HttpMessageNotWritableException(
					"Could not find a suitable converter for message of type " + t.getClass());
		}
		else {
			throw new RuntimeException("TODO");
		}
	}

	public static void main(String[] args) throws IOException {

//		WebsocketClientTransport websocketClientTransport = WebsocketClientTransport
//				.create("35.241.251.246", 80);
		WebsocketClientTransport websocketClientTransport = WebsocketClientTransport
				.create(URI.create("ws://kmprssr.default.35.241.251.246.nip.io/ws"));
//		websocketClientTransport = WebsocketClientTransport
//				.create(URI.create("ws://localhost:8080/ws"));

		RSocket rSocket = RSocketFactory
				.connect()
				.transport(websocketClientTransport)
				.start()
				.block();
		ClientFunctionInvoker<String, String> fn = new ClientFunctionInvoker<>(rSocket, String.class, String.class);

		Flux<String> input = Flux.just("riff", "is", "for", "functions");
		Flux<String> output = fn.apply(input);
		output.log().subscribe(System.out::println);

		System.in.read();
	}
}
