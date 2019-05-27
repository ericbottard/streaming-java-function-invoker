package io.projectriff.invoker;

import java.io.IOException;
import java.io.InputStream;

import io.projectriff.invoker.rpc.InputFrame;
import io.projectriff.invoker.rpc.InputSignal;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.MediaType;

public class NextHttpInputMessage implements HttpInputMessage {

	private final InputSignal signal;

	public NextHttpInputMessage(InputSignal signal) {
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
		headers.set("RiffInput", ""+ data.getArgIndex());
		headers.setContentType(MediaType.parseMediaType(data.getContentType()));
		return headers;
	}
}
