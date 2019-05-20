package io.projectriff.invoker;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.projectriff.invoker.client.ClientFunctionInvoker;
import io.projectriff.invoker.server.Next;
import io.projectriff.invoker.server.ReactorRiffGrpc;
import io.projectriff.invoker.server.Signal;
import io.projectriff.invoker.server.Start;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.hamcrest.MatcherAssert.assertThat;

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
        } else {
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
     * This tests a function that uses reactor types in its signature
     * and shares fluxes in its implementation
     */
    @Test
    public void testStreamingFunctionSharingFluxes() throws Exception {
        setFunctionLocation("repeater-1.0.0-boot");
        setFunctionBean("com.acme.Repeater");
        process = processBuilder.start();

        Signal start = Signal.newBuilder().setStart(Start.newBuilder().setAccept("text/plain").build()).build();
        Flux<String> strings = Flux.just("one", "two", "three");
        Flux<Integer> ints = Flux.just(1, 2, 3, 4, 5, 6);

        Flux<Signal> request = Flux.concat(
                Flux.just(start),
                Flux.merge(strings.map(s -> inputSignal(s, 0)), ints.map(i -> inputSignal(i.toString(), 1)))
        );

        List<Flux<Tuple2<Integer, String>>> result = ReactorRiffGrpc.newReactorStub(connect()).invoke(request)
                .groupBy((s) -> Integer.parseInt(s.getNext().getHeadersOrThrow("RiffOutput")))
                .sort(Comparator.comparing(GroupedFlux::key))
                .map(g -> g.map(s -> Tuples.of(g.key(), s.getNext().getPayload().toStringUtf8())))
                .collectList()
                .block();

        assertThat(result.size(), CoreMatchers.equalTo(2));
        StepVerifier.create(result.get(0))
                .expectNext(Tuples.of(0, "one"))
                .expectNext(Tuples.of(0, "two"))
                .expectNext(Tuples.of(0, "two"))
                .expectNext(Tuples.of(0, "three"))
                .expectNext(Tuples.of(0, "three"))
                .expectNext(Tuples.of(0, "three"))
                .verifyComplete();
        StepVerifier.create(result.get(1))
                .expectNext(Tuples.of(1, "3"))
                .expectNext(Tuples.of(1, "5"))
                .expectNext(Tuples.of(1, "7"))
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
                .verifyErrorMatches(t -> (t instanceof StatusRuntimeException) && ((StatusRuntimeException) t).getStatus().getCode() == Status.Code.CANCELLED);

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
                .verifyErrorMatches(t -> (t instanceof StatusRuntimeException) && ((StatusRuntimeException) t).getStatus().getCode() == Status.Code.UNKNOWN);

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
            } catch (IOException e) {
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

    private Signal inputSignal(String s, int inputIndex) {
        return Signal.newBuilder().setNext(Next.newBuilder()
                .putHeaders("RiffInput", "" + inputIndex)
                .putHeaders("Content-Type", "text/plain")
                .setPayload(ByteString.copyFromUtf8(s))).build();
    }

}
