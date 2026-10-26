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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoRequest;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoResponse;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.ResponseCode;
import io.debezium.connector.oracle.util.TestHelper;

/**
 * A packet that fails to deserialize must be reported without the row values it carries, neither in the
 * message nor anywhere in the cause chain, while still saying where the packet and the error are.
 */
public class OlrNetworkClientDeserializeErrorTest {

    private static final String ROW_VALUE = "Kund Hemlig AB";
    private static final String ROW_TOKEN = "HEMLIGTOKEN4711";
    private static final String HEADER = "{\"scn\":120,\"tm\":0,\"c_scn\":300,\"c_idx\":2,\"xid\":\"0x0001.002.00000003\",\"db\":\"S1\",";

    @Test
    public void shouldNotExposeRowValuesWhenStringIsMalformed() throws Exception {
        // an unescaped control character inside a row value
        final String packet = HEADER + "\"payload\":[{\"op\":\"c\",\"after\":{\"NAME\":\"" + ROW_VALUE + "\u0001" + ROW_VALUE + "\"}}]}";
        assertErrorHidesRowValues(packet, ROW_VALUE);
    }

    @Test
    public void shouldNotExposeRowValuesWhenTokenIsUnrecognized() throws Exception {
        final String packet = HEADER + "\"payload\":[{\"op\":\"c\",\"after\":{\"NAME\":\"" + ROW_VALUE + "\",\"CODE\":" + ROW_TOKEN + "}}]}";
        assertErrorHidesRowValues(packet, ROW_TOKEN);
    }

    private void assertErrorHidesRowValues(String packet, String rowValue) throws Exception {
        final byte[] body = packet.getBytes(StandardCharsets.UTF_8);
        final int errorOffset = packet.indexOf(rowValue.equals(ROW_TOKEN) ? ROW_TOKEN : "\u0001");
        final Throwable error = readInvalidPacket(body);

        assertThat(error).isInstanceOf(OlrNetworkClientException.class);
        assertThat(error.getCause()).isInstanceOf(JsonProcessingException.class);
        assertThat(error.getMessage())
                .contains("length=" + body.length)
                .contains("scn=120")
                .contains("c_scn=300")
                .contains("c_idx=2")
                .containsPattern("offset=" + errorOffset + "\\b");

        final StringWriter trace = new StringWriter();
        error.printStackTrace(new PrintWriter(trace));
        assertThat(trace.toString()).doesNotContain(ROW_VALUE).doesNotContain(ROW_TOKEN).doesNotContain("Hemlig");
    }

    private Throwable readInvalidPacket(byte[] body) throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            final Thread olr = new Thread(() -> serve(server, body));
            olr.setDaemon(true);
            olr.start();

            final OracleConnectorConfig config = new OracleConnectorConfig(TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.OLR_HOST, "127.0.0.1")
                    .with(OracleConnectorConfig.OLR_PORT, server.getLocalPort())
                    .with(OracleConnectorConfig.OLR_SOURCE, "S1")
                    .build());
            final OlrNetworkClient client = new OlrNetworkClient(config);
            try {
                assertThat(client.connect(Scn.valueOf(150), null)).isTrue();
                return catchThrowable(() -> {
                    for (int i = 0; i < 5; i++) {
                        client.readEvent();
                    }
                });
            }
            finally {
                client.disconnect();
            }
        }
    }

    private static void serve(ServerSocket server, byte[] body) {
        try (Socket socket = server.accept()) {
            final DataInputStream in = new DataInputStream(socket.getInputStream());
            final OutputStream out = socket.getOutputStream();
            readRequest(in);
            write(out, RedoResponse.newBuilder().setCode(ResponseCode.READY).build().toByteArray());
            readRequest(in);
            write(out, RedoResponse.newBuilder().setCode(ResponseCode.REPLICATE).build().toByteArray());
            write(out, body);
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
}
