/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleErrorHandler;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoRequest;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoResponse;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RequestCode;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.ResponseCode;
import io.debezium.connector.oracle.util.TestHelper;

/**
 * OpenLogReplicator serves one client at a time. While it still holds the connection of a client that vanished,
 * the kernel completes the handshake of a new connection but the server never reads it. The answer to INFO must
 * therefore be awaited with a timeout that fails retriably, while the answer to START, which can take minutes
 * while the server loads its dictionary, must be awaited without one.
 */
public class OlrNetworkClientHandshakeTimeoutTest {

    @Test
    public void shouldFailRetriablyWhenServerAcceptsButNeverAnswersInfo() throws Exception {
        // Never accepted: the kernel completes the TCP handshake from the backlog, nobody reads
        try (ServerSocket server = new ServerSocket(0, 1)) {
            final OracleConnectorConfig config = config(server, 500);
            final OlrNetworkClient client = new OlrNetworkClient(config);
            final long start = System.nanoTime();
            assertThatThrownBy(() -> client.connect(Scn.valueOf(100), null))
                    .isInstanceOf(OlrNetworkClientException.class)
                    .hasMessageContaining("did not answer the INFO request within 500 ms")
                    .hasCauseInstanceOf(SocketTimeoutException.class)
                    .satisfies(e -> assertThat(new RetriableProbe(config).retriable(e)).isTrue());
            final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            // The timeout is checked between reads, which wait at most one second each
            assertThat(elapsedMs).isBetween(500L, 5_000L);
            assertThat(client.getChannel().isOpen()).isFalse();
            assertThat(client.isConnected()).isFalse();
        }
    }

    @Test
    public void shouldWaitForStartAnswerLongerThanInfoTimeout() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            final Thread olr = new Thread(() -> {
                try (Socket socket = server.accept()) {
                    final DataInputStream in = new DataInputStream(socket.getInputStream());
                    final OutputStream out = socket.getOutputStream();
                    assertThat(readRequest(in).getCode()).isEqualTo(RequestCode.INFO);
                    write(out, RedoResponse.newBuilder().setCode(ResponseCode.READY).build().toByteArray());
                    assertThat(readRequest(in).getCode()).isEqualTo(RequestCode.START);
                    // As when the server loads its dictionary before it answers
                    Thread.sleep(3_000);
                    write(out, RedoResponse.newBuilder().setCode(ResponseCode.REPLICATE).build().toByteArray());
                    socket.getInputStream().read();
                }
                catch (IOException | InterruptedException e) {
                    // the test fails on the client side
                }
            });
            olr.setDaemon(true);
            olr.start();

            final OlrNetworkClient client = new OlrNetworkClient(config(server, 500));
            try {
                assertThat(client.connect(Scn.valueOf(100), null)).isTrue();
                assertThat(client.isConnected()).isTrue();
            }
            finally {
                client.disconnect();
            }
        }
    }

    @Test
    public void shouldAcceptInfoAnswerWithinTimeout() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            final Thread olr = new Thread(() -> {
                try (Socket socket = server.accept()) {
                    final DataInputStream in = new DataInputStream(socket.getInputStream());
                    final OutputStream out = socket.getOutputStream();
                    readRequest(in);
                    Thread.sleep(1_500);
                    write(out, RedoResponse.newBuilder().setCode(ResponseCode.READY).build().toByteArray());
                    readRequest(in);
                    write(out, RedoResponse.newBuilder().setCode(ResponseCode.REPLICATE).build().toByteArray());
                    socket.getInputStream().read();
                }
                catch (IOException | InterruptedException e) {
                    // the test fails on the client side
                }
            });
            olr.setDaemon(true);
            olr.start();

            final OlrNetworkClient client = new OlrNetworkClient(config(server, 3_000));
            try {
                assertThat(client.connect(Scn.valueOf(100), null)).isTrue();
            }
            finally {
                client.disconnect();
            }
        }
    }

    @Test
    public void shouldStopWithoutErrorWhenInterruptedAwaitingInfo() throws Exception {
        try (ServerSocket server = new ServerSocket(0, 1)) {
            final OlrNetworkClient client = new OlrNetworkClient(config(server, 60_000));
            final AtomicReference<Object> result = new AtomicReference<>();
            final AtomicBoolean interruptKept = new AtomicBoolean();
            final CountDownLatch done = new CountDownLatch(1);
            final Thread task = new Thread(() -> {
                try {
                    result.set(client.connect(Scn.valueOf(100), null));
                }
                catch (RuntimeException e) {
                    result.set(e);
                }
                interruptKept.set(Thread.currentThread().isInterrupted());
                done.countDown();
            });
            task.start();
            Thread.sleep(500);
            task.interrupt();
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(result.get()).isEqualTo(false);
            assertThat(interruptKept.get()).isTrue();
            assertThat(client.getChannel().isOpen()).isFalse();
        }
    }

    @Test
    public void shouldRejectInfoTimeoutOutsideRange() {
        assertThat(OracleConnectorConfig.OLR_INFO_TIMEOUT_MS.defaultValue()).isEqualTo(30_000L);
        assertThat(valid(OracleConnectorConfig.OLR_INFO_TIMEOUT_MS, "1")).isTrue();
        assertThat(valid(OracleConnectorConfig.OLR_INFO_TIMEOUT_MS, "2147483647")).isTrue();
        assertThat(valid(OracleConnectorConfig.OLR_INFO_TIMEOUT_MS, "0")).isFalse();
        assertThat(valid(OracleConnectorConfig.OLR_INFO_TIMEOUT_MS, "-1")).isFalse();
        assertThat(valid(OracleConnectorConfig.OLR_INFO_TIMEOUT_MS, "2147483648")).isFalse();
        assertThat(valid(OracleConnectorConfig.OLR_INFO_TIMEOUT_MS, "soon")).isFalse();
    }

    private static OracleConnectorConfig config(ServerSocket server, long infoTimeoutMs) {
        return new OracleConnectorConfig(TestHelper.defaultConfig()
                .with(OracleConnectorConfig.OLR_HOST, "127.0.0.1")
                .with(OracleConnectorConfig.OLR_PORT, server.getLocalPort())
                .with(OracleConnectorConfig.OLR_SOURCE, "S1")
                .with(OracleConnectorConfig.OLR_INFO_TIMEOUT_MS, infoTimeoutMs)
                .build());
    }

    private static boolean valid(Field field, String value) {
        return field.validate(Configuration.create().with(field, value).build(), (f, v, message) -> {
        });
    }

    private static RedoRequest readRequest(InputStream in) throws IOException {
        final byte[] size = in.readNBytes(4);
        final byte[] body = in.readNBytes(ByteBuffer.wrap(size).order(ByteOrder.LITTLE_ENDIAN).getInt());
        return RedoRequest.parseFrom(body);
    }

    private static void write(OutputStream out, byte[] body) throws IOException {
        out.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(body.length).array());
        out.write(body);
        out.flush();
    }

    /**
     * Exposes the connector's retriable classification.
     */
    private static class RetriableProbe extends OracleErrorHandler {
        RetriableProbe(OracleConnectorConfig config) {
            super(config, null, null);
        }

        boolean retriable(Throwable error) {
            return isRetriable(error);
        }
    }
}
