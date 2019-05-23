package io.projectriff.invoker.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.springframework.core.ResolvableType;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.ObjectToStringHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

class HttpServerHandler extends SimpleChannelInboundHandler<Object> {

    private final MethodHandle methodHandle;

    private final Class<?>[] inputTypes;

    private final List<HttpMessageConverter> converters = new ArrayList<>();

    public HttpServerHandler(Method method, Object function) throws Exception {
        MethodHandle methodHandle = MethodHandles.publicLookup().unreflect(method);
        this.methodHandle = methodHandle.bindTo(function);
        Class<?>[] inputTypes = new Class[method.getParameterCount()];
        for (int i = 0; i < method.getParameterCount(); i++) {
            ResolvableType type = ResolvableType.forMethodParameter(method, i);
            // if (!type.isAssignableFrom(FLUX_TYPE)) {
            // throw new RuntimeException("Expected parameter of type Flux at position " + i + ": " +
            // m);
            // }
            inputTypes[i] = type.resolveGeneric(0);
        }
        this.inputTypes = inputTypes;
        converters.add(new MappingJackson2HttpMessageConverter());
        converters.add(new FormHttpMessageConverter());
        StringHttpMessageConverter sc = new StringHttpMessageConverter();
        sc.setWriteAcceptCharset(false);
        converters.add(sc);
        ObjectToStringHttpMessageConverter oc = new ObjectToStringHttpMessageConverter(new DefaultConversionService());
        oc.setWriteAcceptCharset(false);
        converters.add(oc);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, Object msg) throws Exception {
        System.out.println(msg.getClass());
        System.out.println(msg);
        if (!(msg instanceof FullHttpRequest)) {
            return;
        }
        FullHttpRequest request = (FullHttpRequest) msg;
        ByteBuf content = request.content();
        // TODO: methodHandle.invokeWithArguments
        System.err.println("content");
        System.err.println(content.toString(StandardCharsets.UTF_8));

        try {

            MediaType mediaType = MediaType.parseMediaType(request.headers().getAsString("Content-Type"));
            for (HttpMessageConverter converter : converters) {
                if (converter.canRead(inputTypes[0], mediaType)) {
                    converter.read(inputTypes[0], new HttpInputMessage() {
                        @Override
                        public InputStream getBody() throws IOException {
                            // return content.;
                            return;
                        }

                        @Override
                        public HttpHeaders getHeaders() {
                            return null;
                        }
                    });
                }
            }
            String contents = content.toString(StandardCharsets.UTF_8);
            Object result = methodHandle.invokeWithArguments(Flux.just(Integer.parseInt(contents)));
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK,
                    Unpooled.copiedBuffer(result.toString(), StandardCharsets.UTF_8));

            context.write(response);
            context.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        } catch (Throwable throwable) {
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
