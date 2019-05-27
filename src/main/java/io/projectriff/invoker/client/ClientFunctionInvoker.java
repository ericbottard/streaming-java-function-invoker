package io.projectriff.invoker.client;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.projectriff.invoker.HttpMessageUtils;
import io.projectriff.invoker.OutputSignalHttpInputMessage;
import io.projectriff.invoker.SignalHttpOutputMessage;
import io.projectriff.invoker.rpc.InputSignal;
import io.projectriff.invoker.rpc.OutputSignal;
import io.projectriff.invoker.rpc.ReactorRiffGrpc;
import io.projectriff.invoker.rpc.StartFrame;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.projectriff.invoker.HttpMessageUtils.CONTENT_TYPE;
import static io.projectriff.invoker.HttpMessageUtils.RIFF_INPUT;

/**
 * A helper for invoking riff functions from the client side.
 *
 * <p>
 * Takes care of calling the gRPC remote end with the appropriate Start signal,
 * marshalling the input and un-marshalling the invocation result.
 * </p>
 *
 * @author Eric Bottard
 * @deprecated use {@code FunctionProxy} instead
 */
@Deprecated
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

        HttpMessageUtils.installDefaultConverters(converters);
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
        InputSignal start = InputSignal.newBuilder()
                .setStart(StartFrame.newBuilder().addExpectedContentTypes(accept).build())
                .build();

        Flux<InputSignal> request = Flux.concat(
                Flux.just(start),
                input.map(this::convertToSignal)
        );

        return stub.invoke(request).map(this::convertFromSignal);
    }

    private R convertFromSignal(OutputSignal signal) {
        String ct = signal.getData().getHeadersOrThrow(CONTENT_TYPE);
        MediaType contentType = MediaType.parseMediaType(ct);
        OutputSignalHttpInputMessage inputMessage = new OutputSignalHttpInputMessage(signal);
        try {
            for (HttpMessageConverter converter : converters) {
                if (converter.canRead(outputType, contentType)) {
                    return (R) converter.read(outputType, inputMessage);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        throw new HttpMessageNotReadableException("Could not find suitable converter", inputMessage);
    }

    private InputSignal convertToSignal(T t) {
        try {
            for (HttpMessageConverter converter : converters) {
                for (Object mediaType : converter.getSupportedMediaTypes()) {
                    if (converter.canWrite(t.getClass(), (MediaType) mediaType)) {
                        SignalHttpOutputMessage outputMessage = new SignalHttpOutputMessage();
                        converter.write(t, (MediaType) mediaType, outputMessage);
                        outputMessage.getHeaders().add(RIFF_INPUT, "0");
                        return outputMessage.asInputSignal();
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        throw new HttpMessageNotWritableException(
                "Could not find a suitable converter for message of type " + t.getClass());
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
