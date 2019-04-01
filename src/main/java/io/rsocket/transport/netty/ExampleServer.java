package io.rsocket.transport.netty;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.SocketAcceptor;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ExampleServer {

	public static void main(String[] args) {
		RSocketFactory.receive()
				.acceptor(new PingHandler())
				.transport(TcpServerTransport.create(7878))
				.start()
				.block()
				.onClose()
				.block();

	}

	private static class PingHandler implements SocketAcceptor {

		@Override
		public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {

			Mono<Payload> response = sendingSocket.requestResponse(DefaultPayload.create("request"));

			response.doOnNext(p -> System.out.println(p.getDataUtf8()));

			return Mono.just(new AbstractRSocket() {

				@Override
				public Flux<Payload> requestStream(Payload payload) {
					System.out.println("Payload "+  payload.getDataUtf8());
					return Flux.just(DefaultPayload.create("hello from serverside"));
				}
			});
		}
	}
}
