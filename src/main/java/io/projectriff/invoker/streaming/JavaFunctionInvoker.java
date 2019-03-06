package io.projectriff.invoker.streaming;

import java.util.function.Function;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.function.context.config.ContextFunctionCatalogAutoConfiguration;
import org.springframework.cloud.function.deployer.ApplicationBootstrap;
import org.springframework.cloud.function.deployer.EnableFunctionDeployer;
import org.springframework.context.annotation.Bean;

@SpringBootApplication(/*exclude = ContextFunctionCatalogAutoConfiguration.class*/)
@EnableFunctionDeployer
public class JavaFunctionInvoker {

	public static void main(String[] args) throws InterruptedException {
		new ApplicationBootstrap().run(JavaFunctionInvoker.class, "--function.runner.isolated=false");
		Object o = new Object();
		synchronized (o) {
			o.wait();
		}
	}

	@Bean(initMethod = "start", destroyMethod = "shutdown")
	public Server server(FunctionInspector fi, FunctionRegistry registry) {
		return ServerBuilder.forPort(8080).addService(invokerAdapter(fi, registry)).build();
	}

	@Bean
	public BindableService invokerAdapter(FunctionInspector fi, FunctionRegistry registry) {
		Function function = registry.lookup(Function.class, "function0");
		return new ReactorServerThirdAdapter(function, fi);
	}

}
