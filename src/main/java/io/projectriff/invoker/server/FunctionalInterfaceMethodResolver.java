package io.projectriff.invoker.server;

import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Flux;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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
        AtomicReference<Method> refOnFunctionInterface = new AtomicReference<>();
        Class<?> functionalInterface = functionalInterfaces.iterator().next();
        ReflectionUtils.doWithLocalMethods(functionalInterface, m -> {
            if (!m.isBridge() && !m.isSynthetic() && !m.isDefault() && Modifier.isAbstract(m.getModifiers())) {
                if (!refOnFunctionInterface.compareAndSet(null, m)) {
                    throw new RuntimeException("More than one matching method");
                };
            }
        });

        AtomicReference<Method> refActual = new AtomicReference<>();
        ReflectionUtils.doWithMethods(target.getClass(), me -> refActual.set(me),
                overridesMethod(refOnFunctionInterface.get()));

        return refActual.get();
    }

    private ReflectionUtils.MethodFilter overridesMethod(Method fMethod) {
        System.out.println("fMethod = " + fMethod);
        return me -> {
            boolean b = me.getName().equals(fMethod.getName())
                    && !me.isBridge() && !me.isSynthetic()
                    && me.getParameterCount() == fMethod.getParameterCount();
            System.out.println("Considering " + me + " => " + b);
            return b;
        };
    }
}
