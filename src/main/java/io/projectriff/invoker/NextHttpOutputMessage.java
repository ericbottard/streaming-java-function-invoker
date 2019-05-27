package io.projectriff.invoker;

import java.io.IOException;
import java.io.OutputStream;

import com.google.protobuf.ByteString;
import io.projectriff.invoker.rpc.OutputFrame;
import io.projectriff.invoker.rpc.OutputSignal;

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

		public OutputSignal asSignal() {
			OutputFrame.Builder data = OutputFrame.newBuilder()
					.setPayload(output.toByteString())
					.putAllHeaders(headers.toSingleValueMap())
					.setContentType(headers.getContentType().toString())
					.removeHeaders("Content-Type")
					.setResultIndex(Integer.parseInt(headers.getFirst("RiffOuput")))
					.removeHeaders("RiffOutput");
			return OutputSignal.newBuilder().setData(data).build();
		}

	}
