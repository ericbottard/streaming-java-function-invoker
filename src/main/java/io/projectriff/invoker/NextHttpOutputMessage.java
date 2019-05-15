package io.projectriff.invoker;

import java.io.IOException;
import java.io.OutputStream;

import com.google.protobuf.ByteString;
import io.projectriff.invoker.server.Next;
import io.projectriff.invoker.server.Signal;

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

		public Signal asSignal() {
			Next.Builder next = Next.newBuilder()
					.setPayload(output.toByteString())
					.putAllHeaders(headers.toSingleValueMap())
					.putHeaders("RiffInput", "0");
			return Signal.newBuilder().setNext(next).build();
		}

	}
