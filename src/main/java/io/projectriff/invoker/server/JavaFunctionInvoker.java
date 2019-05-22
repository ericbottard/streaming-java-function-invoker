package io.projectriff.invoker.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.deployer.EnableFunctionDeployer;
import org.springframework.context.annotation.Bean;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

/**
 * This class sets up all the necessary infrastructure for exposing a (streaming) function over riff gRPC protocol.
 *
 * <p>Heavy lifting is done via Spring Cloud Function and the function deployer and then the located function
 * is adapted to reactive-grpc server.</p>
 *
 * @author Eric Bottard
 */
@SpringBootApplication
@EnableFunctionDeployer
public class JavaFunctionInvoker {

    @Bean
    public HackyFunctionResolver functionResolver(FunctionRegistry functionRegistry) {
        return new HackyFunctionResolver(functionRegistry);
    }

    /*
     * Exposes an object capable of running a gRPC server with the function.
     * Startup is done in an init methodHandle to work around late initialization needs of the function deployer.
     */
    @Bean(initMethod = "run", destroyMethod = "close")
    public GrpcRunner grpcRunner(HackyFunctionResolver resolver) {
        return new GrpcRunner(resolver);
    }

    /*
     * Exposes an object capable of running a gRPC server with the function.
     * Startup is done in an init methodHandle to work around late initialization needs of the function deployer.
     */
    @Bean(initMethod = "run")
    public HttpRunner httpRunner(HackyFunctionResolver resolver) {
        return new HttpRunner(resolver);
    }

    private static class GrpcRunner {

        private final HackyFunctionResolver resolver;

        private Server server;

        GrpcRunner(HackyFunctionResolver resolver) {
            this.resolver = resolver;
        }

        public void run() throws Exception {
            Object function = resolver.resolveFunction();
            Method m = new FunctionalInterfaceMethodResolver().resolve(function);
            ReactorServerAdapter adapter = new ReactorServerAdapter(function, m);

            server = ServerBuilder.forPort(9090).addService(adapter).build();
            server.start();
        }

        public void close() {
            server.shutdown();
        }

    }

    private static class HttpRunner {

        private static final int PORT = 8080;
        private final HackyFunctionResolver resolver;

        HttpRunner(HackyFunctionResolver resolver) {
            this.resolver = resolver;
        }

        public void run() throws Exception {
            Object function = resolver.resolveFunction();
            Method m = new FunctionalInterfaceMethodResolver().resolve(function);
            MethodHandle mh = MethodHandles.publicLookup().unreflect(m);
            mh = mh.bindTo(function);

            // Configure the server.
            EventLoopGroup bossGroup = new NioEventLoopGroup(1);
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new CustomChannelInitializer(new MethodHandler(mh)));

            Channel ch = b.bind(PORT).sync().channel();

            ch.closeFuture().sync();
        }
    }

    private static class CustomChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final MethodHandler methodHandler;

        public CustomChannelInitializer(JavaFunctionInvoker.MethodHandler methodHandler) {
            this.methodHandler = methodHandler;
        }

        @Override
        protected void initChannel(SocketChannel socketChannel) throws Exception {
            ChannelPipeline pipeline = socketChannel.pipeline();
            pipeline.addLast(new HttpRequestDecoder());
            pipeline.addLast(new HttpResponseEncoder());
            pipeline.addLast(methodHandler);
        }
    }

    private static class MethodHandler extends SimpleChannelInboundHandler<Object> {

        private final MethodHandle methodHandle;

        public MethodHandler(MethodHandle methodHandle) {
            this.methodHandle = methodHandle;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object msg) throws Exception {
            if (msg instanceof HttpContent) {
                ByteBuf content = ((HttpContent) msg).content();
                // TODO: methodHandle.invokeWithArguments
            }
            if (msg instanceof HttpRequest) {
                // TODO ((HttpRequest) msg).???
            }

            DefaultFullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK,
                    Unpooled.copiedBuffer("hello world", StandardCharsets.UTF_8));

            channelHandlerContext.write(response);
            channelHandlerContext.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            ctx.close();
        }
    }
}