package io.projectriff.invoker.server;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.function.deployer.EnableFunctionDeployer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

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

		@Autowired private ApplicationContext applicationContext;

		Runner(FunctionInspector fi, FunctionRegistry registry) {
			this.fi = fi;
			this.registry = registry;
		}

		public void run() throws Exception {
			//Object function = applicationContext.getBean("function");
			//Object function = Class.forName(System.getenv("FUNCTION_BEAN")).getDeclaredConstructor().newInstance();

			Field processor1 = registry.getClass().getDeclaredField("processor");
			processor1.setAccessible(true);
			Object processor = processor1.get(registry);

			Field registry = processor.getClass().getDeclaredField("names");
			registry.setAccessible(true);
			Map map = (Map) registry.get(processor);

			Object function = map.keySet().iterator().next();
			System.out.println("WOOOOOOOOOOT " + function);

			Set<Class<?>> functionalInterfaces = ClassUtils.getAllInterfacesAsSet(function).stream()
					.filter(i -> AnnotationUtils.isAnnotationDeclaredLocally(FunctionalInterface.class, i))
					.collect(Collectors.toSet());
			if (functionalInterfaces.size() == 0) {
				throw new RuntimeException("Could not find any function");
			}
			else if (functionalInterfaces.size() > 1) {
				throw new RuntimeException("Too many functional interfaces implemented: " + functionalInterfaces);
			}
			Method m = functionalInterfaces.iterator().next().getDeclaredMethods()[0];

			MethodType applyType = MethodType.methodType(Object.class, Object.class);
			MethodHandle mh = MethodHandles.publicLookup().unreflect(m);
			mh = mh.bindTo(function);

			ReactorServerAdapter adapter = new ReactorServerAdapter(mh, fi);
			server = ServerBuilder.forPort(8080).addService(adapter).build();
			server.start();
		}

		public void close() {
			server.shutdown();
		}

	}

}
