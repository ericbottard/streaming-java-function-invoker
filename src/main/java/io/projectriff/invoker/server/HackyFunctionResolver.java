package io.projectriff.invoker.server;

import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.function.core.FluxFunction;
import org.springframework.cloud.function.core.Isolated;
import org.springframework.cloud.function.core.IsolatedFunction;
import org.springframework.core.ResolvableType;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.function.Function;

import reactor.core.publisher.Flux;

public class HackyFunctionResolver {

    private final FunctionRegistry functionRegistry;

    private final FunctionInspector functionInspector;

    public HackyFunctionResolver(FunctionRegistry functionRegistry, FunctionInspector fi) {
        this.functionRegistry = functionRegistry;
        functionInspector = fi;
    }

    public Object resolveFunction() throws Exception {
        Field processor1 = functionRegistry.getClass().getDeclaredField("processor");
        processor1.setAccessible(true);
        Object processor = processor1.get(functionRegistry);

        Field registry = processor.getClass().getDeclaredField("names");
        registry.setAccessible(true);
        Map<?, ?> map = (Map) registry.get(processor);

        Object o = map.keySet().stream()
                .filter(HackyFunctionResolver::isFluxifying)
                .findFirst()
                .get();
        System.out.println("Amongst " + map.toString().replace(",", "\n"));
        System.out.println("Electing: " + o);
        return o;
    }

    public Class[] resolveInputTypes(Object function, Method m) {
        Class<?> result = functionInspector.getOutputType(function);
        if (function instanceof Function && result != Object.class) {
            return new Class[]{result};
        }


        // Extra type not handled by SCF such as BiFunction etc.
        Class<?>[] types = new Class[m.getParameterCount()];
        for (int i = 0; i < m.getParameterCount(); i++) {
            ResolvableType type = ResolvableType.forMethodParameter(m, i);
            // if (!type.isAssignableFrom(FLUX_TYPE)) {
            // throw new RuntimeException("Expected parameter of type Flux at position " + i + ": " +
            // m);
            // }
            types[i] = type.resolveGeneric(0);
        }
        return types;

    }

    private static boolean isFluxifying(Object fn) {
        if (!(fn instanceof Function)) {
            return true; // Not a SCF supported Function, assume it's a @FunctionalInterface with Fluxes
        }
        if (fn instanceof FluxFunction) {
            return true;
        }
        if (fn instanceof IsolatedFunction) {
            return false;
        }
        Method m = new FunctionalInterfaceMethodResolver().resolve(fn);
        return m.getParameterTypes()[0].isAssignableFrom(Flux.class);
    }
}
