package io.projectriff.invoker.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.deployer.EnableFunctionDeployer;
import org.springframework.context.annotation.Bean;

import java.lang.reflect.Method;

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

            // Configure the server.
            EventLoopGroup bossGroup = new NioEventLoopGroup(1);
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new CustomChannelInitializer(new HttpServerHandler(m, function)));

            Channel ch = b.bind(PORT).sync().channel();

            ch.closeFuture().sync();
        }
    }

    private static class CustomChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final HttpServerHandler methodHandler;

        public CustomChannelInitializer(HttpServerHandler methodHandler) {
            this.methodHandler = methodHandler;
        }

        @Override
        protected void initChannel(SocketChannel socketChannel) throws Exception {
            ChannelPipeline pipeline = socketChannel.pipeline();
            pipeline.addLast(new HttpRequestDecoder());
            pipeline.addLast(new HttpResponseEncoder());
            pipeline.addLast(new HttpObjectAggregator(Integer.MAX_VALUE));
            pipeline.addLast(methodHandler);
        }
    }

}