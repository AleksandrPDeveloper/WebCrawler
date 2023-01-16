package com.udacity.webcrawler.profiler;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Objects;

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;

/**
 * Concrete implementation of the {@link Profiler}.
 */
final class ProfilerImpl implements Profiler {

    private final Clock clock;
    private final ProfilingState state = new ProfilingState();
    private final ZonedDateTime startTime;

    @Inject
    ProfilerImpl(Clock clock) {
        this.clock = Objects.requireNonNull(clock);
        this.startTime = ZonedDateTime.now(clock);
    }

    @Override
    public <T> T wrap(Class<T> klass, T delegate) {
        Objects.requireNonNull(klass);
        if (Arrays.stream(klass.getMethods()).noneMatch(a -> a.isAnnotationPresent(Profiled.class))){
            throw new IllegalArgumentException();
        }
        Object proxy =
                Proxy.newProxyInstance(
                        klass.getClassLoader(),
                        new Class<?>[]{klass},
                        new ProfilingMethodInterceptor(clock, state, delegate));
        return (T) proxy;
    }

    @Override
    public void writeData(Path path) {
       var option = Files.exists(path)? StandardOpenOption.APPEND: StandardOpenOption.CREATE;

        try (Writer writer = Files.newBufferedWriter(Path.of(path.toUri()), StandardCharsets.UTF_8, option)) {
            writeData(writer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeData(Writer writer) throws IOException {
        writer.write("Run at " + RFC_1123_DATE_TIME.format(startTime));
        writer.write(System.lineSeparator());
        state.write(writer);
        writer.write(System.lineSeparator());
    }
}
