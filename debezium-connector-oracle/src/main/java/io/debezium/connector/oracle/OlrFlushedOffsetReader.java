/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the offsets Kafka Connect has flushed on a thread of its own, waiting at most a timeout, so that
 * a slow or stuck offset store delays a CONFIRM to OpenLogReplicator instead of stopping the poll loop
 * that streams records and heartbeats.
 *
 * <p>At most one read runs at a time. A read that outlives the timeout is left running and no other is
 * started while it does; its result is returned by the first call after it completed. Every result is an
 * offset that was flushed, so a late one only confirms less than a newer one would, never more.
 *
 * <p>{@link #read()} is not thread safe; the connector task calls it under its commit lock.
 *
 * @param <T> the type of the offsets read
 */
class OlrFlushedOffsetReader<T> implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(OlrFlushedOffsetReader.class);

    private final Supplier<T> reader;
    private final Duration timeout;
    private final ExecutorService executor;
    private Future<T> pending;
    private long pendingSince;

    /**
     * @param reader reads the flushed offsets; may block, runs on the executor's thread
     * @param timeout how long {@link #read()} waits for a read it started
     * @param executor runs the reads, one at a time; shut down by {@link #close()}
     */
    OlrFlushedOffsetReader(Supplier<T> reader, Duration timeout, ExecutorService executor) {
        this.reader = reader;
        this.timeout = timeout;
        this.executor = executor;
    }

    /**
     * Returns the flushed offsets, if a read completes in time.
     *
     * @return the offsets of the newest read that completed, or empty when none completed, failed or the
     *         reader was closed
     */
    Optional<T> read() {
        if (executor.isShutdown()) {
            return Optional.empty();
        }
        Optional<T> late = Optional.empty();
        if (pending != null) {
            if (!pending.isDone()) {
                LOGGER.warn("Reading the flushed offset has been running for {} ms; nothing is confirmed to OpenLogReplicator "
                        + "until it completes", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - pendingSince));
                return Optional.empty();
            }
            late = result(pending);
            pending = null;
        }

        final Future<T> read;
        try {
            read = executor.submit(reader::get);
        }
        catch (RejectedExecutionException e) {
            LOGGER.debug("The flushed offset reader is closed");
            return Optional.empty();
        }
        pending = read;
        pendingSince = System.nanoTime();
        try {
            final T offsets = read.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            pending = null;
            return Optional.ofNullable(offsets);
        }
        catch (TimeoutException e) {
            LOGGER.warn("Reading the flushed offset took more than {} ms; streaming goes on and the offset is confirmed to "
                    + "OpenLogReplicator at an offset commit after the read completes", timeout.toMillis());
            return late;
        }
        catch (ExecutionException e) {
            pending = null;
            LOGGER.warn("Couldn't read the flushed offset to confirm it to OpenLogReplicator; the next offset commit tries again",
                    e.getCause());
            return late;
        }
        catch (InterruptedException e) {
            // The read goes on; its result is used by the next call
            Thread.currentThread().interrupt();
            return late;
        }
    }

    private Optional<T> result(Future<T> read) {
        try {
            return Optional.ofNullable(read.get());
        }
        catch (ExecutionException e) {
            LOGGER.warn("Couldn't read the flushed offset", e.getCause());
            return Optional.empty();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        }
        catch (CancellationException e) {
            return Optional.empty();
        }
    }

    /**
     * Stops the reader thread, interrupting a read that is still running.
     */
    @Override
    public void close() {
        executor.shutdownNow();
    }
}
