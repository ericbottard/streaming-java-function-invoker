package io.projectriff.invoker.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.function.core.IsolatedFunction;
import org.springframework.cloud.function.deployer.EnableFunctionDeployer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;

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
	 * Startup is done in an init method to work around late initialization needs of the function deployer.
	 */
	@Bean(initMethod = "run", destroyMethod = "close")
	public Runner runner(FunctionInspector fi, FunctionRegistry registry) {
		return new Runner(fi, registry);

	}

	private static class Runner {

		private final FunctionInspector fi;

		private final FunctionRegistry registry;

		private Server server;

		Runner(FunctionInspector fi, FunctionRegistry registry) {
			this.fi = fi;
			this.registry = registry;
		}

		public void run() throws Exception {
			Object function = lookupFunction();
			Method m = new FunctionalInterfaceMethodResolver().resolve(function);
			ReactorServerAdapter adapter = new ReactorServerAdapter(function, m, fi);

			server = ServerBuilder.forPort(8080).addService(adapter).build();
			server.start();
		}

		private Object lookupFunction() throws NoSuchFieldException, IllegalAccessException {
			Field processor1 = registry.getClass().getDeclaredField("processor");
			processor1.setAccessible(true);
			Object processor = processor1.get(registry);

			Field registry = processor.getClass().getDeclaredField("names");
			registry.setAccessible(true);
			Map<?, ?> map = (Map) registry.get(processor);

			Optional<?> result = map.keySet().stream()
					.filter(f -> !(f instanceof IsolatedFunction<?, ?>))
					.findFirst();
			return result.get();
		}

		public void close() {
			server.shutdown();
		}

	}

}
