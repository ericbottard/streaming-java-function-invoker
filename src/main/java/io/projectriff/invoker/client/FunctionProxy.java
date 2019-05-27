package io.projectriff.invoker.client;

import io.grpc.ManagedChannel;
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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.reflect.Proxy.newProxyInstance;

/**
 * A helper for invoking riff functions from the client side.
 *
 * <p>
 * Takes care of calling the gRPC remote end with the appropriate Start signal,
 * marshalling the input and un-marshalling the invocation result.
 * </p>
 *
 * @author Florent Biville
 * @author Eric Bottard
 */
public class FunctionProxy {

    private FunctionProxy() {
    }

    @SuppressWarnings("unchecked")
    public static <T> T create(Class<T> type, ManagedChannel channel, Class<?>... outputTypes) {
        Method method = new MethodResolver().resolve(type); // CHANGEME

        return (T) newProxyInstance(
                type.getClassLoader(),
                new Class[]{type},
                new FunctionInvocationHandler(channel, method, outputTypes)
        );
    }

    private static class FunctionInvocationHandler implements InvocationHandler {

        private final ReactorRiffGrpc.ReactorRiffStub riffStub;

        private final List<HttpMessageConverter> converters = new ArrayList<>();

        private String[] acceptHeaders;

        private final Method method;

        private final Class<?>[] outputTypes;

        public FunctionInvocationHandler(ManagedChannel channel, Method method, Class<?>[] outputTypes) {
            this.riffStub = ReactorRiffGrpc.newReactorStub(channel);
            this.method = method;
            this.outputTypes = outputTypes;

            HttpMessageUtils.installDefaultConverters(converters);
            computeAcceptHeaders();
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            if (!method.equals(this.method)) {
                return null; //FIXME?
            }

            InputSignal start = InputSignal.newBuilder()
                    .setStart(StartFrame.newBuilder()
                            .addAllExpectedContentTypes(Arrays.asList(acceptHeaders))
                            .build())
                    .build();


            Flux<InputSignal> allInputSignals = Flux.empty();
            for (int i = 0; i < args.length; i++) {
                final int inputNumber = i;
                allInputSignals = allInputSignals
                        .mergeWith(((Flux<?>) args[i]).map(t -> toNextSignal(inputNumber, t)));
            }

            Flux<OutputSignal> response = riffStub.invoke(Flux.concat(
                    Flux.just(start),
                    allInputSignals
            ));

            return response
                    .groupBy(sig -> sig.getData().getResultIndex())
                    .map(g -> g.map(s -> convertFromSignal(s, outputTypes[g.key()])))
                    .take(outputTypes.length)
                    .collectList()
                    .block()
                    .toArray(Flux[]::new);
        }

        private void computeAcceptHeaders() {
            this.acceptHeaders = Arrays.stream(this.outputTypes)
                    .map(outputType -> MediaType.toString(
                            converters
                                    .stream()
                                    .filter(c -> c.canRead(outputType, null))
                                    .flatMap(this::getSupportedMediaTypes)
                                    .distinct()
                                    .sorted(MediaType.SPECIFICITY_COMPARATOR)
                                    .collect(Collectors.toList()))
                    )
                    .toArray(String[]::new);
        }

        private <T> T convertFromSignal(OutputSignal signal, Class<T> outputType) {
            String ct = signal.getData().getContentType();
            MediaType contentType = MediaType.parseMediaType(ct);
            OutputSignalHttpInputMessage inputMessage = new OutputSignalHttpInputMessage(signal);
            try {
                for (HttpMessageConverter converter : converters) {
                    if (converter.canRead(outputType, contentType)) {
                        return (T) converter.read(outputType, inputMessage);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            throw new HttpMessageNotReadableException("Could not find suitable converter", inputMessage);
        }

        private Stream<MediaType> getSupportedMediaTypes(HttpMessageConverter converter) {
            List<MediaType> supportedMediaTypes = converter.getSupportedMediaTypes();
            // This drops charsets
            return supportedMediaTypes.stream().map(mt -> new MediaType(mt.getType(), mt.getSubtype()));
        }

        private InputSignal toNextSignal(int inputNumber, Object payload) {
            if (payload == null) {
                throw new RuntimeException("TODO");
            }
            try {
                for (HttpMessageConverter converter : converters) {
                    for (Object mediaType : converter.getSupportedMediaTypes()) {
                        if (converter.canWrite(payload.getClass(), (MediaType) mediaType)) {
                            SignalHttpOutputMessage outputMessage = new SignalHttpOutputMessage();
                            converter.write(payload, (MediaType) mediaType, outputMessage);
                            outputMessage.getHeaders().add(HttpMessageUtils.RIFF_INPUT, "" + inputNumber);
                            return outputMessage.asInputSignal();
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            throw new HttpMessageNotWritableException(
                    "Could not find a suitable converter for message of type " + payload.getClass());
        }
    }
}

