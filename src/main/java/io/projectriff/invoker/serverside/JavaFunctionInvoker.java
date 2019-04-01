package io.projectriff.invoker.serverside;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.projectriff.invoker.server.RiffServer;
import io.rsocket.Closeable;
import io.rsocket.RSocketFactory;
import io.rsocket.rpc.rsocket.RequestHandlingRSocket;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.WebsocketRouteTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRoutes;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.function.deployer.EnableFunctionDeployer;
import org.springframework.context.annotation.Bean;

/**
 * This class sets up all the necessary infrastructure for exposing a (streaming) function
 * over riff rsocket-rpc protocol.
 *
 * <p>
 * Heavy lifting is done via Spring Cloud Function and the function deployer and then the
 * located function is adapted to rsocket-rpc serverside.
 * </p>
 *
 * @author Eric Bottard
 */
@SpringBootApplication
@EnableFunctionDeployer
public class JavaFunctionInvoker {

	@Bean(initMethod = "run", destroyMethod = "close")
	public Runner runner(FunctionInspector fi, FunctionRegistry registry) {
		return new Runner(fi, registry);

	}

	private static class Runner {

		private final FunctionInspector fi;

		private final FunctionRegistry registry;

		private Closeable closeableChannel;

		Runner(FunctionInspector fi, FunctionRegistry registry) {
			this.fi = fi;
			this.registry = registry;
		}

		public void run() throws IOException {

			Function function = registry.lookup(Function.class, "function0");
			RiffAdapter riffAdapter = new RiffAdapter(function, fi);
			RiffServer riffServer = new RiffServer(riffAdapter, Optional.empty(), Optional.empty());

			Consumer<HttpServerRoutes> routeSetup = routes -> {
				routes.get("/", (req, res) -> {
					System.out.println(req);
					return res.send(Flux.just(ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "Hello"))).then();
				});
			};

			HttpServer httpServer = HttpServer.create().host("localhost").port(8080);

			closeableChannel = RSocketFactory
					.receive()
					.acceptor((setup, sendingSocket) -> Mono.just(
							new RequestHandlingRSocket(riffServer)))
					.transport(new WebsocketRouteTransport(httpServer, routeSetup, "/ws"))
					.start()
					.block();

		}

		public void close() {
			closeableChannel.dispose();
		}

	}

}
