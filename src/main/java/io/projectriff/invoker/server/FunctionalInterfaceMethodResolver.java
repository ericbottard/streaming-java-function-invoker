package io.projectriff.invoker.server;

import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Flux;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An implementation of {@link FunctionMethodResolver} that looks at all implemented interfaces of the object
 * and selects the method that implements the (sole) {@code @}{@link FunctionalInterface} contract.
 *
 * @author Eric Bottard
 */
public class FunctionalInterfaceMethodResolver implements FunctionMethodResolver {
    @Override
    public Method resolve(Object target) {
        Set<Class<?>> functionalInterfaces = ClassUtils.getAllInterfacesAsSet(target).stream()
                .filter(i -> AnnotationUtils.isAnnotationDeclaredLocally(FunctionalInterface.class, i))
                .collect(Collectors.toSet());
        if (functionalInterfaces.size() == 0) {
            throw new RuntimeException("Could not find any function");
        }
        else if (functionalInterfaces.size() > 1) {
            throw new RuntimeException("Too many functional interfaces implemented: " + functionalInterfaces);
        }
        Method fMethod = functionalInterfaces.iterator().next().getDeclaredMethods()[0];

        AtomicReference<Method> ref = new AtomicReference<>();
        ReflectionUtils.doWithMethods(target.getClass(), me -> ref.set(me),
                overridesMethod(fMethod));

        return ref.get();
    }

    private ReflectionUtils.MethodFilter overridesMethod(Method fMethod) {
        return me -> me.getName().equals(fMethod.getName())
                && !me.isBridge() && !me.isSynthetic()
                && me.getParameterCount() == fMethod.getParameterCount()
                ;
    }
}
