package io.projectriff.invoker.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.projectriff.invoker.NextHttpInputMessage;
import io.projectriff.invoker.NextHttpOutputMessage;
import io.projectriff.invoker.server.ReactorRiffGrpc;
import io.projectriff.invoker.server.Signal;
import io.projectriff.invoker.server.Start;
import reactor.core.publisher.Flux;

import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.http.HttpHeaders;
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
 * A helper for invoking riff functions from the client side.
 *
 * <p>
 * Takes care of calling the gRPC remote end with the appropriate Start signal,
 * marshalling the input and un-marshalling the invocation result.
 * </p>
 *
 * @author Eric Bottard
 */
public class ClientFunctionInvoker<T, R> implements Function<Flux<T>, Flux<R>> {

	private final ReactorRiffGrpc.ReactorRiffStub stub;

	private final List<HttpMessageConverter> converters = new ArrayList<>();

	private String accept;

	private final Class<T> inputType;

	private final Class<R> outputType;

	public ClientFunctionInvoker(Channel channel, Class<T> inputType, Class<R> outputType) {
		stub = ReactorRiffGrpc.newReactorStub(channel);
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
		Signal start = Signal.newBuilder().setStart(Start.newBuilder().setAccept(accept).build()).build();

		Flux<Signal> request = Flux.concat(
				Flux.just(start),
				input.map(this::convertToSignal) //
		);
		Flux<Signal> response = stub.invoke(request);
		return response.map(this::convertFromSignal);
	}

	private R convertFromSignal(Signal signal) {
		String ct = signal.getNext().getHeadersOrThrow("Content-Type");
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

	private Signal convertToSignal(T t) {
		if (t != null) {
			try {
				for (HttpMessageConverter converter : converters) {
					for (Object mediaType : converter.getSupportedMediaTypes()) {
						if (converter.canWrite(t.getClass(), (MediaType) mediaType)) {
							NextHttpOutputMessage outputMessage = new NextHttpOutputMessage();
							converter.write(t, (MediaType) mediaType, outputMessage);
							return outputMessage.asSignal();
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
		ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080)
				.usePlaintext()
				.overrideAuthority("encode.default.example.com")
				.build();
		ClientFunctionInvoker<Integer, Integer> fn = new ClientFunctionInvoker<>(channel, Integer.class, Integer.class);

		Flux<Integer> input = Flux.just(1, 1, 1, 0, 0, 1, 1, 1);
		Flux<Integer> output = fn.apply(input);
		output.log().subscribe(System.out::println);

		System.in.read();
	}
}
