package io.projectriff.invoker;

import java.io.IOException;
import java.io.InputStream;

import io.projectriff.invoker.rpc.InputFrame;
import io.projectriff.invoker.rpc.InputSignal;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.MediaType;

import static io.projectriff.invoker.HttpMessageUtils.RIFF_INPUT;

/**
 * An implementation of {@link HttpInputMessage} that can be constructed from an {@link InputSignal}.
 *
 * Used on the serverside to decode the invocation request.
 *
 * @author Eric Bottard
 */
public class InputSignalHttpInputMessage implements HttpInputMessage {

	private final InputSignal signal;

	public InputSignalHttpInputMessage(InputSignal signal) {
		this.signal = signal;
	}

	@Override
	public InputStream getBody() throws IOException {
		return signal.getData().getPayload().newInput();
	}

	@Override
	public HttpHeaders getHeaders() {
		HttpHeaders headers = new HttpHeaders();
		InputFrame data = signal.getData();
		data.getHeadersMap().forEach((k, v) -> headers.set(k, v));
		headers.set(RIFF_INPUT, ""+ data.getArgIndex());
		headers.setContentType(MediaType.parseMediaType(data.getContentType()));
		return headers;
	}
}
