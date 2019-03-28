package io.projectriff.invoker;

import java.io.IOException;
import java.io.OutputStream;

import com.google.protobuf.ByteString;
import io.projectriff.invoker.server.Message;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpOutputMessage;

public class NextHttpOutputMessage implements HttpOutputMessage {

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

		public Message asMessage() {
			return Message.newBuilder()
							.setPayload(output.toByteString())
							.putAllHeaders(headers.toSingleValueMap())
					.build();
		}

	}
