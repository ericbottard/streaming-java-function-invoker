package io.projectriff.invoker;

import java.io.IOException;
import java.io.InputStream;

import io.projectriff.invoker.server.Message;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;

public class NextHttpInputMessage implements HttpInputMessage {

	private final Message message;

	public NextHttpInputMessage(Message message) {
		this.message = message;
	}

	@Override
	public InputStream getBody() throws IOException {
		return message.getPayload().newInput();
	}

	@Override
	public HttpHeaders getHeaders() {
		HttpHeaders headers = new HttpHeaders();
		message.getHeadersMap().forEach((k, v) -> headers.set(k, v));
		return headers;
	}
}
