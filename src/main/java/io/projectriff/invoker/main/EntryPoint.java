package io.projectriff.invoker.main;

import io.projectriff.invoker.streaming.JavaFunctionInvoker;

import org.springframework.cloud.function.deployer.ApplicationBootstrap;

public class EntryPoint {

	public static void main(String[] args) throws InterruptedException {
		new ApplicationBootstrap().run(JavaFunctionInvoker.class);
		Object o = new Object();
		synchronized (o) {
			o.wait();
		}
	}

}
