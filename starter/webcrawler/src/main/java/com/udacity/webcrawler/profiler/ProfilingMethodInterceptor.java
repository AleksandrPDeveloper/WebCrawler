package com.udacity.webcrawler.profiler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Objects;

/**
 * A method interceptor that checks whether {@link Method}s are annotated with the {@link Profiled}
 * annotation. If they are, the method interceptor records how long the method invocation took.
 */
final class ProfilingMethodInterceptor implements InvocationHandler {

    private final Clock clock;
    private final ProfilingState state;
    private final Object delegate;


    ProfilingMethodInterceptor(Clock clock, ProfilingState state, Object delegate) {
        this.clock = Objects.requireNonNull(clock);
        this.state = state;
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Object obj;
        if (method.isAnnotationPresent(Profiled.class)) {
            ZonedDateTime startTime = ZonedDateTime.now(clock);
            try {
                obj = method.invoke(delegate, args);
                return obj;
            } catch (Exception e) {
                if (e instanceof InvocationTargetException ex) {
                    throw ex.getTargetException();
                } else
                    throw new RuntimeException(e);
            } finally {
                state.record(delegate.getClass(), method, Duration.between(startTime, ZonedDateTime.now(clock)));
            }
        } else {
            try {
                return method.invoke(delegate, args);
            } catch (Exception e) {
                if (e instanceof InvocationTargetException ex) {
                    throw ex.getTargetException();
                } else
                    throw new RuntimeException(e);
            }
        }
    }
}

