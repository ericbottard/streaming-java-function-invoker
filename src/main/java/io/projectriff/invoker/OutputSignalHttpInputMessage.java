package io.projectriff.invoker;

import io.projectriff.invoker.rpc.OutputFrame;
import io.projectriff.invoker.rpc.OutputSignal;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.MediaType;

import java.io.IOException;
import java.io.InputStream;

import static io.projectriff.invoker.HttpMessageUtils.RIFF_OUTPUT;

/**
 * An implementation of {@link HttpInputMessage} backed by an {@link OutputSignal}.
 *
 * Used on the client side to decode the invocation response.
 *
 * @author Eric Bottard
 */
public class OutputSignalHttpInputMessage implements HttpInputMessage {

    private final OutputSignal signal;

    public OutputSignalHttpInputMessage(OutputSignal signal) {
        this.signal = signal;
    }

    @Override
    public InputStream getBody() throws IOException {
        return signal.getData().getPayload().newInput();
    }

    @Override
    public HttpHeaders getHeaders() {
        HttpHeaders headers = new HttpHeaders();
        OutputFrame data = signal.getData();
        data.getHeadersMap().forEach((k, v) -> headers.set(k, v));
        headers.set(RIFF_OUTPUT, "" + data.getResultIndex());
        headers.setContentType(MediaType.parseMediaType(data.getContentType()));
        return headers;
    }
}
