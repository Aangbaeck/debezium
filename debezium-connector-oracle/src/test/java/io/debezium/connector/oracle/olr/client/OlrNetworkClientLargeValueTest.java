/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.olr.client.payloads.InsertEvent;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoRequest;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoResponse;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.ResponseCode;
import io.debezium.connector.oracle.util.TestHelper;

/**
 * A LOB column value can be longer than Jackson's default maximum string length of 20,000,000 characters
 * (a CLOB, or a BLOB over 10 MB sent as hex). The client must parse such a message instead of failing.
 */
public class OlrNetworkClientLargeValueTest {

    private static final int VALUE_LENGTH = 20_000_001;

    @Test
    public void shouldParseStringValueLongerThanJacksonDefaultLimit() throws Exception {
        final char[] chars = new char[VALUE_LENGTH];
        Arrays.fill(chars, 'A');
        final String value = new String(chars);
        final byte[] message = ("{\"scn\":200,\"tm\":0,\"c_scn\":200,\"c_idx\":1,\"xid\":\"0x0001.002.00000003\",\"db\":\"S1\","
                + "\"payload\":[{\"op\":\"c\",\"schema\":{\"owner\":\"DEBEZIUM\",\"table\":\"T\",\"obj\":1},"
                + "\"after\":{\"DOC\":\"" + value + "\"}}]}").getBytes(StandardCharsets.UTF_8);

        try (ServerSocket server = new ServerSocket(0)) {
            final Thread olr = new Thread(() -> serve(server, message));
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
                StreamingEvent event = null;
                for (int i = 0; i < 30 && event == null; i++) {
                    event = client.readEvent();
                }
                assertThat(event).isNotNull();
                final InsertEvent insert = (InsertEvent) event.getPayload().get(0);
                assertThat((String) insert.getAfter().getValues().get("DOC")).hasSize(VALUE_LENGTH);
            }
            finally {
                client.disconnect();
            }
        }
    }

    private static void serve(ServerSocket server, byte[] message) {
        try (Socket socket = server.accept()) {
            final DataInputStream in = new DataInputStream(socket.getInputStream());
            final OutputStream out = socket.getOutputStream();
            readRequest(in);
            write(out, RedoResponse.newBuilder().setCode(ResponseCode.READY).build().toByteArray());
            readRequest(in);
            write(out, RedoResponse.newBuilder().setCode(ResponseCode.REPLICATE).build().toByteArray());
            write(out, message);
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
