package io.projectriff.invoker.server;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;

import io.rsocket.RSocketFactory;
import io.rsocket.rpc.rsocket.RequestHandlingRSocket;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import reactor.core.publisher.Mono;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.function.deployer.EnableFunctionDeployer;
import org.springframework.context.annotation.Bean;

/**
 * This class sets up all the necessary infrastructure for exposing a (streaming) function over riff rsocket-rpc protocol.
 *
 * <p>Heavy lifting is done via Spring Cloud Function and the function deployer and then the located function
 * is adapted to rsocket-rpc server.</p>
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

		private CloseableChannel closeableChannel;

		Runner(FunctionInspector fi, FunctionRegistry registry) {
			this.fi = fi;
			this.registry = registry;
		}

		public void run() throws IOException {
			Function function = registry.lookup(Function.class, "function0");
			RiffAdapter riffAdapter = new RiffAdapter(function, fi);
			RiffServer riffServer = new RiffServer(riffAdapter, Optional.empty(), Optional.empty());
			closeableChannel = RSocketFactory
					.receive()
					.acceptor((setup, sendingSocket) -> Mono.just(
							new RequestHandlingRSocket(riffServer)
					))
					.transport(WebsocketServerTransport.create(8080))
					.start()
					.block();

		}

		public void close() {
			closeableChannel.dispose();
		}

	}

}
