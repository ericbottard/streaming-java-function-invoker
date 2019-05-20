package io.projectriff.invoker.client;

import io.projectriff.invoker.server.FunctionMethodResolver;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

public class MethodResolver {

    public Method resolve(Class<?> type) {
        List<Method> methods = Arrays.stream(ReflectionUtils.getAllDeclaredMethods(type))
                .filter(m -> !m.isBridge() && !m.isSynthetic() && !m.isDefault() && !Modifier.isStatic(m.getModifiers()))
                .collect(Collectors.toList());

        if (methods.isEmpty()) {
            throw new RuntimeException("No functional interface method found");
        }
        if (methods.size() > 1) {
            String culprits = methods.stream().map(Method::toString).collect(joining(", "));
            throw new RuntimeException(String.format("Too many functional interface methods found: %s", culprits));
        }
        return methods.iterator().next();
    }
}
