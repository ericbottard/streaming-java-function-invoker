package io.projectriff.invoker.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.projectriff.invoker.HttpMessageUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ChannelHandler.Sharable
class HttpServerHandler extends SimpleChannelInboundHandler<Object> {

	private final MethodHandle methodHandle;

	private final Class<?>[] inputTypes;

	private final List<HttpMessageConverter> converters = new ArrayList<>();

	public HttpServerHandler(Method method, Object function, Class[] types) throws Exception {
		MethodHandle methodHandle = MethodHandles.publicLookup().unreflect(method);
		this.methodHandle = methodHandle.bindTo(function);
		this.inputTypes = types;

		HttpMessageUtils.installDefaultConverters(converters);
	}

	@Override
	protected void channelRead0(ChannelHandlerContext context, Object msg) throws Exception {
		if (!(msg instanceof FullHttpRequest)) {
			return;
		}
		FullHttpRequest request = (FullHttpRequest) msg;
		ByteBuf content = request.content();

		try {

			io.netty.handler.codec.http.HttpHeaders headers = request.headers();
			MediaType contentType = MediaType.parseMediaType(headers.getAsString(HttpHeaderNames.CONTENT_TYPE));
			List<MediaType> accept = MediaType.parseMediaTypes(headers.getAsString(HttpHeaderNames.ACCEPT));

			Object arg = null;
			HttpInputMessage inputMessage = new HttpInputMessage() {

				@Override
				public InputStream getBody() throws IOException {
					return new ByteBufInputStream(content);
				}

				@Override
				public HttpHeaders getHeaders() {
					var h = new HttpHeaders();
					for (Map.Entry<String, String> header : headers) {
						h.add(header.getKey(), header.getValue());
					}
					return h;
				}
			};
			for (HttpMessageConverter converter : converters) {
				if (converter.canRead(inputTypes[0], contentType)) {
					arg = converter.read(inputTypes[0], inputMessage);
					break;
				}
			}
			if (arg == null) {
				throw new HttpMessageNotReadableException("Could not read data", inputMessage);
			}
			Flux<?> result = (Flux<?>) methodHandle.invokeWithArguments(Flux.just(arg));
			Object value = result.blockFirst();
//			Object value = methodHandle.invokeWithArguments(arg);
            ByteBuf out = Unpooled.buffer();

            ByteBufOutputStream os = new ByteBufOutputStream(out);
			DefaultFullHttpResponse response = new DefaultFullHttpResponse(
					HttpVersion.HTTP_1_1,
					HttpResponseStatus.OK,
					out);
            HttpHeaders outHeaders = new HttpHeaders();

            boolean wrote = false;
			for (MediaType accepted : accept) {
				for (HttpMessageConverter converter : converters) {
					if (converter.canWrite(value.getClass(), accepted)) {
                        converter.write(value, accepted, new HttpOutputMessage() {

                            @Override
                            public OutputStream getBody() throws IOException {
                                return os;
                            }

                            @Override
                            public HttpHeaders getHeaders() {
                                return outHeaders;
                            }
                        });
                        wrote = true;
                        break;
					}
				}
			}
			if (!wrote) {
				throw new HttpMessageNotWritableException("Could not write response");
			}

			outHeaders.forEach((k, vs) -> response.headers().add(k, vs));
			context.write(response);
			context.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
		}
		catch (Throwable throwable) {
			if (throwable instanceof RuntimeException) {
				throw (RuntimeException) throwable;
			}
			throw new RuntimeException(throwable);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
		ctx.close();
	}
}
