package io.projectriff.invoker.server;

import java.lang.reflect.Method;

/**
 * A strategy interface for locating the "functional" method to invoke.
 *
 * @author Eric Bottard
 */
public interface FunctionMethodResolver {

    Method resolve(Object target);
}
