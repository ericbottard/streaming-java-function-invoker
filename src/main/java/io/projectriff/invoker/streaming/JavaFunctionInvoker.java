package io.projectriff.invoker.streaming;

import java.io.IOException;
import java.util.function.Function;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.deployer.resource.support.DelegatingResourceLoader;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.function.core.FluxFunction;
import org.springframework.cloud.function.deployer.EnableFunctionDeployer;
import org.springframework.context.annotation.Bean;

@SpringBootApplication(/*exclude = ContextFunctionCatalogAutoConfiguration.class*/)
@EnableFunctionDeployer
public class JavaFunctionInvoker {

	@Bean(initMethod = "run", destroyMethod = "close")
	public Runner toto(FunctionInspector fi, FunctionRegistry registry) {
		return new Runner(fi, registry);

	}

	private static class Runner {

		private final FunctionInspector fi;

		private final FunctionRegistry registry;

		private Server server;

		public Runner(FunctionInspector fi, FunctionRegistry registry) {

			this.fi = fi;
			this.registry = registry;
		}

		public void run() throws IOException {
			Function function = registry.lookup(Function.class, "function0");
			System.out.println("WOOOOT " + function);
			ReactorServerThirdAdapter adapter = new ReactorServerThirdAdapter(function, fi);
			server = ServerBuilder.forPort(8080).addService(adapter).build();
			server.start();
		}

		public void close() {
			server.shutdown();
		}


	}

}
