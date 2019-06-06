package io.projectriff.invoker.server;

import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * An implementation of {@link FunctionMethodResolver} that looks at all implemented interfaces of the object
 * and selects the method that implements the contract defined by {@code @}{@link FunctionalInterface}.
 * Note that the presence of {@link FunctionalInterface} is not necessary.
 *
 * @author Eric Bottard
 * @author Florent Biville
 */
public class FunctionalInterfaceMethodResolver implements FunctionMethodResolver {

    private static final List<Method> OBJECT_PUBLIC_METHODS = Arrays.asList(Object.class.getMethods());

    @Override
    public Method resolve(Object target) {
        Method functionalInterfaceMethod = findFunctionalInterfaceMethod(target);

        AtomicReference<Method> refActual = new AtomicReference<>();
        ReflectionUtils.doWithMethods(
                target.getClass(),
                refActual::set,
                me -> isOverriddenBy(functionalInterfaceMethod, me));
        return refActual.get();
    }

    private Method findFunctionalInterfaceMethod(Object target) {
        Set<Class<?>> functionalInterfaces = ClassUtils.getAllInterfacesForClassAsSet(target.getClass()).stream()
                .filter(i -> filterFunctionalMethods(i).count() == 1)
                .collect(Collectors.toSet());

        if (functionalInterfaces.size() == 0) {
            throw new RuntimeException("Could not find any function");
        } else if (functionalInterfaces.size() > 1) {
            throw new RuntimeException("Too many functional interfaces implemented: " + functionalInterfaces.stream().sorted(comparing(Class::getName)).collect(toList()));
        }
        
        Class<?> functionalInterface = functionalInterfaces.iterator().next();
        return filterFunctionalMethods(functionalInterface)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Cannot happen - functional method not found on function interface " + functionalInterface));
    }

    private Stream<Method> filterFunctionalMethods(Class<?> functionalInterface) {
        return Arrays.stream(functionalInterface.getMethods()).filter(this::isFunctionalMethod);
    }

    private boolean isFunctionalMethod(Method method) {
        return method.getDeclaringClass().isInterface() // added for completeness but redundant here
                && Modifier.isAbstract(method.getModifiers())
                && OBJECT_PUBLIC_METHODS.stream().noneMatch(m -> isOverriddenBy(m, method));
    }

    private boolean isOverriddenBy(Method baseMethod, Method overridingMethod) {
        return baseMethod.getDeclaringClass().isAssignableFrom(overridingMethod.getDeclaringClass())
                && overridingMethod.getName().equals(baseMethod.getName());

    }
}