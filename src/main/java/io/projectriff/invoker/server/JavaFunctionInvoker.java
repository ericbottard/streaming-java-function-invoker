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
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.function.deployer.EnableFunctionDeployer;
import org.springframework.cloud.function.deployer.FunctionProperties;
import org.springframework.context.annotation.Bean;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.function.Function;

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

    /*
     * Exposes an object capable of running a gRPC server with the function.
     * Startup is done in an init methodHandle to work around late initialization needs of the function deployer.
     */
    @Bean(initMethod = "run", destroyMethod = "close")
    public GrpcRunner grpcRunner(FunctionCatalog functionCatalog, FunctionProperties functionProperties) {
        return new GrpcRunner(functionCatalog, functionProperties.getName());
    }

    private static class GrpcRunner {

        private Server server;

        public GrpcRunner(FunctionCatalog functionCatalog, String functionName) {
            GrpcServerAdapter adapter = new GrpcServerAdapter(functionCatalog, functionName);
            server = ServerBuilder.forPort(8081).addService(adapter).build();
        }

        public void run() throws Exception {
            server.start();
        }

        public void close() {
            server.shutdown();
        }

    }
}
