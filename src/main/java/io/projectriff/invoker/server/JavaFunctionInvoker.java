package io.projectriff.invoker.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.deployer.resource.maven.MavenProperties;
import org.springframework.cloud.deployer.resource.support.DelegatingResourceLoader;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.deployer.EnableFunctionDeployer;
import org.springframework.cloud.function.deployer.FunctionDeployerConfiguration;
import org.springframework.cloud.function.deployer.FunctionProperties;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.util.ClassUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
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
//@EnableWebMvc
public class JavaFunctionInvoker
        implements ApplicationContextInitializer<GenericApplicationContext> {

//    @Bean
//    public HackyFunctionResolver functionResolver(FunctionRegistry functionRegistry) {
//        return new HackyFunctionResolver(functionRegistry);
//    }


    /*
     * Exposes an object capable of running a gRPC server with the function.
     * Startup is done in an init method to work around late initialization needs of the function deployer.
     */
//    @Bean(initMethod = "run", destroyMethod = "close")
//    public Runner runner(HackyFunctionResolver resolver) {
//        return new Runner(resolver);
//    }

    @Override
    public void initialize(GenericApplicationContext genericApplicationContext) {
        genericApplicationContext.registerBean(Runner.class,
                () -> new Runner(new HackyFunctionResolver(genericApplicationContext.getBean(FunctionRegistry.class))));
        genericApplicationContext.registerBean(FunctionDeployerConfiguration.class, FunctionDeployerConfiguration::new);
        genericApplicationContext.registerBean(
                "org.springframework.cloud.function.deployer.FunctionCreatorConfiguration",
                ClassUtils.resolveClassName(
                        "org.springframework.cloud.function.deployer.FunctionCreatorConfiguration",
                        genericApplicationContext.getClassLoader()));
        genericApplicationContext.registerBean(
                MavenProperties.class, () -> genericApplicationContext
                        .getBean(FunctionDeployerConfiguration.class).mavenProperties(),
                def -> {
                    def.setFactoryBeanName(FunctionDeployerConfiguration.class.getName());
                    def.setFactoryMethodName("mavenProperties");
                });
        genericApplicationContext.registerBean(FunctionProperties.class, () -> genericApplicationContext
                        .getBean(FunctionDeployerConfiguration.class).functionProperties(),
                def -> {
                    def.setFactoryBeanName(FunctionDeployerConfiguration.class.getName());
                    def.setFactoryMethodName("functionProperties");
                });
        genericApplicationContext.registerBean(DelegatingResourceLoader.class,
                () -> genericApplicationContext.getBean(FunctionDeployerConfiguration.class)
                        .delegatingResourceLoader(
                                genericApplicationContext.getBean(MavenProperties.class)));
    }

    private static class Runner {

        private final HackyFunctionResolver resolver;

        private Server server;

        Runner(HackyFunctionResolver resolver) {
            this.resolver = resolver;
        }

        @PostConstruct
        public void run() throws Exception {
            Object function = resolver.resolveFunction();
            Method m = new FunctionalInterfaceMethodResolver().resolve(function);
            ReactorServerAdapter adapter = new ReactorServerAdapter(function, m);

            server = ServerBuilder.forPort(9090).addService(adapter).build();
            server.start();
        }

        @PreDestroy
        public void close() {
            server.shutdown();
        }

    }
}
