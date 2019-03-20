package io.projectriff.invoker;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.projectriff.invoker.client.ClientFunctionInvoker;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class IntegrationTest {

	private static String javaExecutable;

	private static String invokerJar;

	private ProcessBuilder processBuilder;

	private Process process;

	@Rule
	public TestName testName = new TestName();

	private ManagedChannel channel;

	@BeforeClass
	public static void locateJavaExecutable() {
		File exec = new File(System.getProperty("java.home"), "bin/java");
		if (exec.exists()) {
			javaExecutable = exec.getPath();
		}
		else {
			javaExecutable = "java";
		}
	}

	@BeforeClass
	public static void locateInvokerJar() {
		String[] targets = new File("target")
				.list((d, n) -> n.matches("java-function-invoker-\\d+\\.\\d+\\.\\d+(-SNAPSHOT)?\\.jar"));
		if (targets.length != 1) {
			throw new RuntimeException("Could not locate java invoker jar in " + Arrays.asList(targets));
		}
		invokerJar = String.format("target%s%s", File.separator, targets[0]);
	}

	@Before
	public void prepareProcess() {
		processBuilder = new ProcessBuilder(javaExecutable, "-jar", invokerJar);
		processBuilder
				.redirectOutput(new File(String.format("target%s%s.out", File.separator, testName.getMethodName())));
		processBuilder
				.redirectError(new File(String.format("target%s%s.err", File.separator, testName.getMethodName())));
		processBuilder.environment().clear();
		processBuilder.environment().put("PATH", System.getenv("PATH"));
	}

	@After
	public void shutdownInvoker() throws InterruptedException {
		channel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
		process.destroy();
		process.waitFor();
	}

	/*
	 * This tests a function that uses reactor types in its signature.
	 */
	@Test
	public void testStreamingFunction() throws Exception {
		setFunctionLocation("encode-1.0.0-boot");
		setFunctionBean("com.acme.Encode");
		process = processBuilder.start();

		ClientFunctionInvoker<Integer, Integer> fn = new ClientFunctionInvoker<>(connect(), Integer.class, Integer.class);

		Flux<Integer> response = fn.apply(Flux.just(1, 1, 1, 0, 0, 0, 0, 1, 1));
		StepVerifier.create(response)
				.expectNext(3, 1)
				.expectNext(4, 0)
				.expectNext(2, 1)
				.verifyComplete();

	}

	/*
	 * This tests a function that doesn't require special types and is packaged as a plain old
	 * jar.
	 */
	@Test
	public void testSimplestJarFunction() throws Exception {
		setFunctionLocation("hundred-divider-1.0.0");
		setFunctionBean("com.acme.HundredDivider");
		process = processBuilder.start();

		ClientFunctionInvoker<Integer, Integer> fn = new ClientFunctionInvoker<>(connect(), Integer.class, Integer.class);

		Flux<Integer> response = fn.apply(Flux.just(1, 2, 4));
		StepVerifier.create(response)
				.expectNext(100, 50, 25)
				.verifyComplete();

	}

	/*
	 * This tests the client triggering an onError() event.
	 */
	@Test
	public void testClientError() throws Exception {
		setFunctionLocation("hundred-divider-1.0.0");
		setFunctionBean("com.acme.HundredDivider");
		process = processBuilder.start();

		ClientFunctionInvoker<Integer, Integer> fn = new ClientFunctionInvoker<>(connect(), Integer.class, Integer.class);

		Flux<Integer> response = fn.apply(Flux.concat(
				Flux.just(1, 2, 3),
				Flux.error(new RuntimeException("Boom"))) //
		);
		StepVerifier.create(response)
				.verifyErrorMatches(t -> (t instanceof StatusRuntimeException) && ((StatusRuntimeException)t).getStatus().getCode()== Status.Code.CANCELLED);

	}

	/*
	 * This tests a runtime error happening in the function computation.
	 */
	@Test
	public void testFunctionError() throws Exception {
		setFunctionLocation("hundred-divider-1.0.0");
		setFunctionBean("com.acme.HundredDivider");
		process = processBuilder.start();

		ClientFunctionInvoker<Integer, Integer> fn = new ClientFunctionInvoker<>(connect(), Integer.class, Integer.class);

		Flux<Integer> response = fn.apply(Flux.just(1, 2, 0));
		StepVerifier.create(response)
				.expectNext(100, 50)
				.verifyErrorMatches(t -> (t instanceof StatusRuntimeException) && ((StatusRuntimeException)t).getStatus().getCode()== Status.Code.UNKNOWN);

	}

	/**
	 * Waits for connectivity to the gRPC server then creates, sets and returns a Channel that can be used
	 * to create a {@link ClientFunctionInvoker}.
	 */
	private ManagedChannel connect() throws InterruptedException {
		for (int i = 0; i < 20; i++) {
			try {
				new Socket().connect(new InetSocketAddress("localhost", 8080));
				break;
			}
			catch (IOException e) {
				Thread.sleep(500);
			}
		}
		channel = ManagedChannelBuilder.forAddress("localhost", 8080)
				.usePlaintext()
				.build();
		return channel;
	}

	private String setFunctionBean(String value) {
		return processBuilder.environment().put("FUNCTION_BEAN", value);
	}

	private void setFunctionLocation(String jar) {
		processBuilder.environment().put("FUNCTION_LOCATION",
				"file://" + new File(String.format("src/test/functions/%s.jar", jar)).getAbsolutePath());
	}

}
