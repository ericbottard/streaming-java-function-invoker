package io.projectriff.invoker;

import java.io.IOException;
import java.io.OutputStream;

import com.google.protobuf.ByteString;
import io.projectriff.invoker.rpc.InputFrame;
import io.projectriff.invoker.rpc.InputSignal;
import io.projectriff.invoker.rpc.OutputFrame;
import io.projectriff.invoker.rpc.OutputSignal;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpOutputMessage;

import static io.projectriff.invoker.HttpMessageUtils.*;

/**
 * An implementation of {@link HttpOutputMessage} that can be converted to an {@link OutputSignal} or {@link io.projectriff.invoker.rpc.InputSignal}.
 * <p>
 * Used on the server side to produce the invocation response, and on the client side to craft the invocation request.
 *
 * @author Eric Bottard
 */
public class SignalHttpOutputMessage implements HttpOutputMessage {

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

    public OutputSignal asOutputSignal() {
        OutputFrame.Builder data = OutputFrame.newBuilder()
                .setPayload(output.toByteString())
                .putAllHeaders(headers.toSingleValueMap())
                .setContentType(headers.getContentType().toString())
                .removeHeaders(CONTENT_TYPE)
                .setResultIndex(Integer.parseInt(headers.getFirst(RIFF_OUTPUT)))
                .removeHeaders(RIFF_OUTPUT);
        return OutputSignal.newBuilder().setData(data).build();
    }

    public InputSignal asInputSignal() {
        InputFrame.Builder data = InputFrame.newBuilder()
                .setPayload(output.toByteString())
                .putAllHeaders(headers.toSingleValueMap())
                .setContentType(headers.getContentType().toString())
                .removeHeaders(CONTENT_TYPE)
                .setArgIndex(Integer.parseInt(headers.getFirst(RIFF_INPUT)))
                .removeHeaders(RIFF_INPUT);
        return InputSignal.newBuilder().setData(data).build();
    }

}
