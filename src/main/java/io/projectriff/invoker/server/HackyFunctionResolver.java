package io.projectriff.invoker.server;

import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.core.IsolatedFunction;

import java.lang.reflect.Field;
import java.util.Map;

public class HackyFunctionResolver {

    private final FunctionRegistry functionRegistry;

    public HackyFunctionResolver(FunctionRegistry functionRegistry) {
        this.functionRegistry = functionRegistry;
    }

    public Object resolveFunction() throws Exception {
        Field processor1 = functionRegistry.getClass().getDeclaredField("processor");
        processor1.setAccessible(true);
        Object processor = processor1.get(functionRegistry);

        Field registry = processor.getClass().getDeclaredField("names");
        registry.setAccessible(true);
        Map<?, ?> map = (Map) registry.get(processor);

        return map.keySet().stream()
                .filter(f -> !(f instanceof IsolatedFunction<?, ?>))
                .findFirst()
                .get();
    }
}
