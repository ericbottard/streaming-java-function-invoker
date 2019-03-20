package io.projectriff.invoker;

import java.io.IOException;
import java.io.InputStream;

import io.projectriff.invoker.server.Signal;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;

public class NextHttpInputMessage implements HttpInputMessage {

	private final Signal signal;

	public NextHttpInputMessage(Signal signal) {
		this.signal = signal;
	}

	@Override
	public InputStream getBody() throws IOException {
		return signal.getNext().getPayload().newInput();
	}

	@Override
	public HttpHeaders getHeaders() {
		HttpHeaders headers = new HttpHeaders();
		signal.getNext().getHeadersMap().forEach((k, v) -> headers.set(k, v));
		return headers;
	}
}
