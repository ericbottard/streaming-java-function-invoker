package com.acme;

import org.springframework.core.ResolvableType;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.lang.reflect.ParameterizedType;
import java.util.function.Function;

//@SpringBootApplication
public class RepeaterApplication {

    //@Bean
    public Function<Tuple2<Flux<String>, Flux<Integer>>,
            Tuple2<Flux<Double>, Flux<String>>
            > fn() {
        return new MyFn();
    }

    public static void main(String[] args) {
        Class x = MyFn.class;
        MyFn<String, String> y = new MyFn<String, String>();
        System.out.println(ResolvableType.forClass(MyFn.class).getType() instanceof ParameterizedType);

        //SpringApplication.run(RepeaterApplication.class, args);
    }
}
