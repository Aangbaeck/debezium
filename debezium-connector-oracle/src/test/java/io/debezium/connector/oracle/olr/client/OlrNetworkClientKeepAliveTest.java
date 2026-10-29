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
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleErrorHandler;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoRequest;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoResponse;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.ResponseCode;
import io.debezium.connector.oracle.util.TestHelper;

import jdk.net.ExtendedSocketOptions;

/**
 * OpenLogReplicator sends nothing while the database is quiet, so a server that vanished without closing the
 * connection is only noticed through TCP keepalive. The client must enable it with the configured timings, and
 * a connection the operating system fails must surface as a retriable error rather than end the read loop.
 */
public class OlrNetworkClientKeepAliveTest {

    @Test
    public void shouldEnableKeepAliveWithDefaultTimings() throws Exception {
        assertKeepAlive(Configuration.create().build(), 60, 10, 6);
    }

    @Test
    public void shouldEnableKeepAliveWithConfiguredTimings() throws Exception {
        assertKeepAlive(Configuration.create()
                .with(OracleConnectorConfig.OLR_KEEPALIVE_IDLE_MS, 30_000)
                .with(OracleConnectorConfig.OLR_KEEPALIVE_INTERVAL_MS, 5_000)
                .with(OracleConnectorConfig.OLR_KEEPALIVE_COUNT, 3)
                .build(), 30, 5, 3);
    }

    @Test
    public void shouldRoundKeepAliveTimingsUpToWholeSeconds() throws Exception {
        assertKeepAlive(Configuration.create()
                .with(OracleConnectorConfig.OLR_KEEPALIVE_IDLE_MS, 1_500)
                .with(OracleConnectorConfig.OLR_KEEPALIVE_INTERVAL_MS, 1_001)
                .build(), 2, 2, 6);
    }

    @Test
    public void shouldRejectKeepAliveTimingsOutsideSocketLimits() {
        assertThat(valid(OracleConnectorConfig.OLR_KEEPALIVE_IDLE_MS, "60000")).isTrue();
        assertThat(valid(OracleConnectorConfig.OLR_KEEPALIVE_IDLE_MS, "1000")).isTrue();
        assertThat(valid(OracleConnectorConfig.OLR_KEEPALIVE_IDLE_MS, "32767000")).isTrue();
        assertThat(valid(OracleConnectorConfig.OLR_KEEPALIVE_IDLE_MS, "999")).isFalse();
        assertThat(valid(OracleConnectorConfig.OLR_KEEPALIVE_IDLE_MS, "0")).isFalse();
        assertThat(valid(OracleConnectorConfig.OLR_KEEPALIVE_INTERVAL_MS, "32767001")).isFalse();
        assertThat(valid(OracleConnectorConfig.OLR_KEEPALIVE_INTERVAL_MS, "ten")).isFalse();
        assertThat(valid(OracleConnectorConfig.OLR_KEEPALIVE_COUNT, "1")).isTrue();
        assertThat(valid(OracleConnectorConfig.OLR_KEEPALIVE_COUNT, "127")).isTrue();
        assertThat(valid(OracleConnectorConfig.OLR_KEEPALIVE_COUNT, "0")).isFalse();
        assertThat(valid(OracleConnectorConfig.OLR_KEEPALIVE_COUNT, "128")).isFalse();
    }

    @Test
    public void shouldFailRetriablyWhenConnectionIsReset() throws Exception {
        final CountDownLatch streaming = new CountDownLatch(1);
        try (ServerSocket server = new ServerSocket(0)) {
            final Thread olr = new Thread(() -> {
                try (Socket socket = server.accept()) {
                    handshake(socket);
                    streaming.await(10, TimeUnit.SECONDS);
                    // What a probe to a vanished server can get back: a reset instead of a reply
                    socket.setSoLinger(true, 0);
                }
                catch (IOException | InterruptedException e) {
                    // the test fails on the client side
                }
            });
            olr.setDaemon(true);
            olr.start();

            final OlrNetworkClient client = new OlrNetworkClient(config(server, Configuration.create().build()));
            try {
                assertThat(client.connect(Scn.valueOf(100), null)).isTrue();
                streaming.countDown();
                assertThatThrownBy(() -> {
                    for (int i = 0; i < 20; i++) {
                        client.readEvent();
                    }
                })
                        .isInstanceOf(OlrNetworkClientException.class)
                        .hasCauseInstanceOf(IOException.class)
                        .satisfies(e -> assertThat(new RetriableProbe(config(server, Configuration.create().build())).retriable(e)).isTrue());
            }
            finally {
                client.disconnect();
            }
        }
    }

    @Test
    public void shouldTreatKeepAliveTimeoutAsRetriable() {
        // The read error the operating system raises once the keepalive probes go unanswered
        final OlrNetworkClientException error = new OlrNetworkClientException("Failed to fill byte buffer",
                new IOException("Connection timed out"));
        final OracleConnectorConfig config = new OracleConnectorConfig(TestHelper.defaultConfig().build());
        assertThat(new RetriableProbe(config).retriable(error)).isTrue();
    }

    @Test
    public void shouldCloseChannelWhenStartFails() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            final Thread olr = new Thread(() -> {
                try (Socket socket = server.accept()) {
                    // Closed before the handshake
                }
                catch (IOException e) {
                    // the test fails on the client side
                }
            });
            olr.setDaemon(true);
            olr.start();

            final OlrNetworkClient client = new OlrNetworkClient(config(server, Configuration.create().build()));
            boolean connected = false;
            try {
                connected = client.connect(Scn.valueOf(100), null);
            }
            catch (OlrNetworkClientException e) {
                // a failed start may throw or return false
            }
            assertThat(connected).isFalse();
            assertThat(client.getChannel().isOpen()).isFalse();
        }
    }

    private static void assertKeepAlive(Configuration overrides, int idle, int interval, int count) throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            final Thread olr = new Thread(() -> {
                try (Socket socket = server.accept()) {
                    handshake(socket);
                    socket.getInputStream().read();
                }
                catch (IOException e) {
                    // the client closed the connection
                }
            });
            olr.setDaemon(true);
            olr.start();

            final OlrNetworkClient client = new OlrNetworkClient(config(server, overrides));
            try {
                assertThat(client.connect(Scn.valueOf(100), null)).isTrue();
                final SocketChannel channel = client.getChannel();
                assertThat(channel.getOption(StandardSocketOptions.SO_KEEPALIVE)).isTrue();
                assertThat(channel.getOption(ExtendedSocketOptions.TCP_KEEPIDLE)).isEqualTo(idle);
                assertThat(channel.getOption(ExtendedSocketOptions.TCP_KEEPINTERVAL)).isEqualTo(interval);
                assertThat(channel.getOption(ExtendedSocketOptions.TCP_KEEPCOUNT)).isEqualTo(count);
            }
            finally {
                client.disconnect();
            }
        }
    }

    private static OracleConnectorConfig config(ServerSocket server, Configuration overrides) {
        return new OracleConnectorConfig(TestHelper.defaultConfig()
                .with(OracleConnectorConfig.OLR_HOST, "127.0.0.1")
                .with(OracleConnectorConfig.OLR_PORT, server.getLocalPort())
                .with(OracleConnectorConfig.OLR_SOURCE, "S1")
                .with(overrides)
                .build());
    }

    private static boolean valid(Field field, String value) {
        return field.validate(Configuration.create().with(field, value).build(), (f, v, message) -> {
        });
    }

    private static void handshake(Socket socket) throws IOException {
        final DataInputStream in = new DataInputStream(socket.getInputStream());
        final OutputStream out = socket.getOutputStream();
        readRequest(in);
        write(out, RedoResponse.newBuilder().setCode(ResponseCode.READY).build().toByteArray());
        readRequest(in);
        write(out, RedoResponse.newBuilder().setCode(ResponseCode.REPLICATE).build().toByteArray());
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
