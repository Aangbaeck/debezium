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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoRequest;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoResponse;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RequestCode;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.ResponseCode;
import io.debezium.connector.oracle.util.TestHelper;

/**
 * A transaction open at the start SCN commits after it: its first rows carry SCNs below the start SCN, and the
 * server sends them because it selects by commit position. The client must hand them on, after START (server
 * READY) and after CONTINUE (server already streaming), when the offset has no index.
 */
public class OlrNetworkClientStartTest {

    private static final String BELOW_START = "{\"scn\":120,\"tm\":0,\"c_scn\":300,\"c_idx\":2,\"xid\":\"0x0001.002.00000003\",\"db\":\"S1\","
            + "\"payload\":[{\"op\":\"begin\"}]}";

    @Test
    public void shouldEmitRowsBelowStartScnAfterStart() throws Exception {
        assertRowBelowStartScnEmitted(ResponseCode.READY, RequestCode.START);
    }

    @Test
    public void shouldEmitRowsBelowStartScnAfterContinue() throws Exception {
        assertRowBelowStartScnEmitted(ResponseCode.REPLICATE, RequestCode.CONTINUE);
    }

    private void assertRowBelowStartScnEmitted(ResponseCode infoResponse, RequestCode expectedRequest) throws Exception {
        final List<RequestCode> requests = new CopyOnWriteArrayList<>();
        try (ServerSocket server = new ServerSocket(0)) {
            final Thread olr = new Thread(() -> serve(server, infoResponse, requests));
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
                for (int i = 0; i < 5 && event == null; i++) {
                    event = client.readEvent();
                }
                assertThat(requests).containsExactly(RequestCode.INFO, expectedRequest);
                assertThat(event).isNotNull();
                assertThat(event.getScn()).isEqualTo(Scn.valueOf(120));
            }
            finally {
                client.disconnect();
            }
        }
    }

    private static void serve(ServerSocket server, ResponseCode infoResponse, List<RequestCode> requests) {
        try (Socket socket = server.accept()) {
            final DataInputStream in = new DataInputStream(socket.getInputStream());
            final OutputStream out = socket.getOutputStream();
            requests.add(readRequest(in).getCode());
            write(out, RedoResponse.newBuilder().setCode(infoResponse).build().toByteArray());
            requests.add(readRequest(in).getCode());
            write(out, RedoResponse.newBuilder().setCode(ResponseCode.REPLICATE).build().toByteArray());
            write(out, BELOW_START.getBytes(StandardCharsets.UTF_8));
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
