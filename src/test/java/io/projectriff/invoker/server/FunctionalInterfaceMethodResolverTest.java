package io.projectriff.invoker.server;

import org.junit.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FunctionalInterfaceMethodResolverTest {

    FunctionalInterfaceMethodResolver resolver = new FunctionalInterfaceMethodResolver();

    @Test
    public void detects_explicit_functional_interface_method_from_lambda() {
        Method method = resolver.resolve((ExplicitFunctionalInterface) () -> {
        });

        assertThat(method.getName()).isEqualTo("explicit");
    }

    @Test
    public void detects_explicit_functional_interface_method() {
        Method method = resolver.resolve(new SimpleExplicitFunction());

        assertThat(method.getName()).isEqualTo("explicit");
    }

    @Test
    public void detects_explicit_functional_interface_method_in_complex_class_hierarchy() {
        Method method = resolver.resolve(new ChildExplicitFunction());

        assertThat(method.getName()).isEqualTo("explicit");
    }

    @Test
    public void detects_explicit_functional_interface_method_in_complex_interface_hierarchy() {
        Method method = resolver.resolve(new MultiInterfaceExplicitFunction());

        assertThat(method.getName()).isEqualTo("explicit");
    }

    @Test
    public void detects_implicit_functional_interface_method_from_lambda() {
        Method method = resolver.resolve((ImplicitFunctionalInterface) () -> {
        });

        assertThat(method.getName()).isEqualTo("implicit");
    }

    @Test
    public void detects_implicit_functional_interface_method() {
        Method method = resolver.resolve(new SimpleImplicitFunction());

        assertThat(method.getName()).isEqualTo("implicit");
    }

    @Test
    public void detects_implicit_functional_interface_method_in_complex_class_hierarchy() {
        Method method = resolver.resolve(new ChildImplicitFunction());

        assertThat(method.getName()).isEqualTo("implicit");
    }

    @Test
    public void detects_implicit_functional_interface_method_in_complex_interface_hierarchy() {
        Method method = resolver.resolve(new MultiInterfaceImplicitFunction());

        assertThat(method.getName()).isEqualTo("implicit");
    }

    @Test
    public void rejects_multiple_functional_interfaces() {
        String expectedErrorMessage = "Too many functional interfaces implemented: [" +
                "interface io.projectriff.invoker.server.ExplicitFunctionalInterface, " +
                "interface io.projectriff.invoker.server.ImplicitFunctionalInterface" +
                "]";
        assertThatThrownBy(() -> resolver.resolve(new MultipleFunctionalInterfaceFunction()))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    public void rejects_missing_functional_interface() {
        String expectedErrorMessage = "Could not find any function";
        assertThatThrownBy(() -> resolver.resolve(new NoFunctionalInterfaceFunction("foo")))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(expectedErrorMessage);
    }
}

///// explicit functional interface

@FunctionalInterface
interface ExplicitFunctionalInterface {
    void explicit();

    boolean equals(Object obj); // should be filtered out
}

abstract class SuperExplicitClass implements ExplicitFunctionalInterface {

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}

interface ExtraExplicitInterface extends ExplicitFunctionalInterface {

    default void lookAtMyself() {
        this.explicit();
    }
}

class SimpleExplicitFunction implements ExplicitFunctionalInterface {

    @Override
    public void explicit() {
    }

    public void ignoreMe() {
    }
}

class ChildExplicitFunction extends SuperExplicitClass {

    @Override
    public void explicit() {

    }

    @Override
    public String toString() {
        return "ChildExplicitFunction{}";
    }
}

class MultiInterfaceExplicitFunction implements ExtraExplicitInterface {

    @Override
    public void explicit() {

    }
}


///// implicit functional interface


interface ImplicitFunctionalInterface {
    void implicit();

    int hashCode(); // should be filtered out
}

abstract class SuperImplicitClass implements ImplicitFunctionalInterface {

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}

class SimpleImplicitFunction implements ImplicitFunctionalInterface {

    @Override
    public void implicit() {

    }
}

interface ExtraImplicitInterface extends ImplicitFunctionalInterface {

    default void lookAtMyself() {
        this.implicit();
    }
}

class ChildImplicitFunction extends SuperImplicitClass {

    @Override
    public void implicit() {

    }

    @Override
    public String toString() {
        return "ChildImplicitFunction{}";
    }
}

class MultiInterfaceImplicitFunction implements ExtraImplicitInterface {
    @Override
    public void implicit() {

    }
}

///// rejection cases

class MultipleFunctionalInterfaceFunction implements ExplicitFunctionalInterface, ImplicitFunctionalInterface {

    @Override
    public void explicit() {

    }

    @Override
    public void implicit() {

    }
}


class NoFunctionalInterfaceFunction {

    private final String item;

    public NoFunctionalInterfaceFunction(String item) {
        this.item = item;
    }

    public void doSomething(String item) {
        System.out.println("hello");
    }

    @Override
    public String toString() {
        return "NoFunctionalInterfaceFunction{}";
    }
}


