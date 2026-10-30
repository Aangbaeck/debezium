/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleErrorHandler;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoRequest;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoResponse;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.ResponseCode;
import io.debezium.connector.oracle.util.TestHelper;

/**
 * A server that refuses START or CONTINUE refuses it again on every retry, so the task has to fail
 * with an error that is not retried instead of staying idle.
 */
public class OlrNetworkClientRefusedStartTest {

    @Test
    public void shouldFailWhenContinueRefused() throws Exception {
        final Throwable error = connectAgainstRefusingServer(ResponseCode.REPLICATE);
        assertThat(error).isInstanceOf(DebeziumException.class)
                .hasMessageContaining("FAILED_START")
                .hasMessageContaining("CONTINUE after SCN 150 index 0")
                .hasMessageContaining("restart OpenLogReplicator");
        assertThat(new RetriableCheck(error).retriable()).isFalse();
    }

    @Test
    public void shouldFailWhenStartRefused() throws Exception {
        final Throwable error = connectAgainstRefusingServer(ResponseCode.READY);
        assertThat(error).isInstanceOf(DebeziumException.class)
                .hasMessageContaining("FAILED_START")
                .hasMessageContaining("START at SCN 150")
                .hasMessageContaining("restart OpenLogReplicator");
        assertThat(new RetriableCheck(error).retriable()).isFalse();
    }

    private static Throwable connectAgainstRefusingServer(ResponseCode infoResponse) throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            final Thread olr = new Thread(() -> serve(server, infoResponse));
            olr.setDaemon(true);
            olr.start();

            final OlrNetworkClient client = new OlrNetworkClient(config(server.getLocalPort()));
            try {
                return catchThrowable(() -> client.connect(Scn.valueOf(150), null));
            }
            finally {
                client.disconnect();
            }
        }
    }

    private static OracleConnectorConfig config(int port) {
        return new OracleConnectorConfig(TestHelper.defaultConfig()
                .with(OracleConnectorConfig.OLR_HOST, "127.0.0.1")
                .with(OracleConnectorConfig.OLR_PORT, port)
                .with(OracleConnectorConfig.OLR_SOURCE, "S1")
                .build());
    }

    private static void serve(ServerSocket server, ResponseCode infoResponse) {
        try (Socket socket = server.accept()) {
            final DataInputStream in = new DataInputStream(socket.getInputStream());
            final OutputStream out = socket.getOutputStream();
            readRequest(in);
            write(out, RedoResponse.newBuilder().setCode(infoResponse).build().toByteArray());
            readRequest(in);
            write(out, RedoResponse.newBuilder().setCode(ResponseCode.FAILED_START).build().toByteArray());
            in.read();
        }
        catch (IOException e) {
            // the client closed the connection
        }
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

    private static class RetriableCheck extends OracleErrorHandler {
        private final Throwable error;

        RetriableCheck(Throwable error) {
            super(config(0), null, null);
            this.error = error;
        }

        boolean retriable() {
            return isRetriable(error);
        }
    }
}
