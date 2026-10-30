/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The flushed offset is read in the task's poll loop, so the read must be bounded: a stuck offset store may delay
 * a CONFIRM but must not stop streaming, and repeated timeouts must not pile up threads or reads.
 */
public class OlrFlushedOffsetReaderTest {

    private final AtomicInteger threads = new AtomicInteger();
    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        threads.incrementAndGet();
        final Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
    });

    @AfterEach
    void shutdown() {
        executor.shutdownNow();
    }

    @Test
    void shouldReturnOffsetsReadWithinTimeout() {
        final OlrFlushedOffsetReader<String> reader = new OlrFlushedOffsetReader<>(() -> "100", Duration.ofSeconds(5), executor);
        assertThat(reader.read()).contains("100");
        assertThat(reader.read()).contains("100");
    }

    @Test
    void shouldGiveUpAfterTimeoutAndNotStartAnotherReadWhileOneRuns() throws Exception {
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger reads = new AtomicInteger();
        final OlrFlushedOffsetReader<String> reader = new OlrFlushedOffsetReader<>(() -> {
            final int n = reads.incrementAndGet();
            await(release);
            return "read " + n;
        }, Duration.ofMillis(200), executor);

        final long start = System.nanoTime();
        assertThat(reader.read()).isEmpty();
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isLessThan(3_000L);

        // Still stuck: neither waits nor starts another read
        for (int i = 0; i < 50; i++) {
            final long before = System.nanoTime();
            assertThat(reader.read()).isEmpty();
            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - before)).isLessThan(100L);
        }
        assertThat(reads.get()).isEqualTo(1);
        assertThat(threads.get()).isEqualTo(1);

        // Completes late: the next call uses a new read, and the late one when the new one times out too
        release.countDown();
        Thread.sleep(100);
        assertThat(reader.read()).contains("read 2");
        assertThat(reads.get()).isEqualTo(2);
        assertThat(threads.get()).isEqualTo(1);
    }

    @Test
    void shouldReturnLateResultWhenTheNextReadTimesOut() throws Exception {
        final CountDownLatch first = new CountDownLatch(1);
        final CountDownLatch second = new CountDownLatch(1);
        final AtomicInteger reads = new AtomicInteger();
        final OlrFlushedOffsetReader<String> reader = new OlrFlushedOffsetReader<>(() -> {
            final int n = reads.incrementAndGet();
            await(n == 1 ? first : second);
            return "read " + n;
        }, Duration.ofMillis(200), executor);

        assertThat(reader.read()).isEmpty();
        first.countDown();
        Thread.sleep(100);
        assertThat(reader.read()).contains("read 1");
        assertThat(reads.get()).isEqualTo(2);
        second.countDown();
    }

    @Test
    void shouldReadAgainAfterFailure() {
        final AtomicInteger reads = new AtomicInteger();
        final OlrFlushedOffsetReader<String> reader = new OlrFlushedOffsetReader<>(() -> {
            if (reads.incrementAndGet() == 1) {
                throw new IllegalStateException("Failed to fetch offsets.");
            }
            return "100";
        }, Duration.ofSeconds(5), executor);

        assertThat(reader.read()).isEmpty();
        assertThat(reader.read()).contains("100");
    }

    @Test
    void shouldReturnNothingForNoOffsets() {
        final OlrFlushedOffsetReader<String> reader = new OlrFlushedOffsetReader<>(() -> null, Duration.ofSeconds(5), executor);
        assertThat(reader.read()).isEqualTo(Optional.empty());
    }

    @Test
    void shouldInterruptRunningReadAndRefuseNewOnesWhenClosed() throws Exception {
        final CountDownLatch interrupted = new CountDownLatch(1);
        final OlrFlushedOffsetReader<String> reader = new OlrFlushedOffsetReader<>(() -> {
            try {
                Thread.sleep(60_000);
            }
            catch (InterruptedException e) {
                interrupted.countDown();
            }
            return "100";
        }, Duration.ofMillis(100), executor);

        assertThat(reader.read()).isEmpty();
        reader.close();
        assertThat(interrupted.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        assertThat(reader.read()).isEmpty();
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
