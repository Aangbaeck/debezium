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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoRequest;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoResponse;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RequestCode;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.ResponseCode;
import io.debezium.connector.oracle.util.TestHelper;

/**
 * CONTINUE carries the position the client continues after, which may differ from the SCN a START reads redo
 * from, and CONFIRM is sent for every position past the last one confirmed, including the first and a later
 * index at the same SCN.
 */
public class OlrNetworkClientConfirmTest {

    @Test
    public void shouldContinueAfterGivenPositionAndConfirmOnlyForward() throws Exception {
        final List<RedoRequest> requests = new CopyOnWriteArrayList<>();
        try (ServerSocket server = new ServerSocket(0)) {
            final Thread olr = new Thread(() -> serve(server, ResponseCode.REPLICATE, requests));
            olr.setDaemon(true);
            olr.start();

            final OlrNetworkClient client = new OlrNetworkClient(config(server));
            try {
                assertThat(client.connect(Scn.valueOf(100), Scn.valueOf(120), 3L)).isTrue();
                client.confirm(Scn.valueOf(120), 3L);
                client.confirm(Scn.valueOf(120), 5L);
                client.confirm(Scn.valueOf(120), 5L);
                client.confirm(Scn.valueOf(120), 0L);
                client.confirm(Scn.valueOf(110), 9L);
                client.confirm(Scn.valueOf(130), 0L);

                Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> requests.size() >= 5);
                Thread.sleep(200);

                assertThat(requests.get(1).getCode()).isEqualTo(RequestCode.CONTINUE);
                assertThat(requests.get(1).getCScn()).isEqualTo(120L);
                assertThat(requests.get(1).getCIdx()).isEqualTo(3L);
                assertThat(requests.subList(2, requests.size()))
                        .extracting(r -> r.getCode() + " " + r.getCScn() + " " + r.getCIdx())
                        .containsExactly("CONFIRM 120 3", "CONFIRM 120 5", "CONFIRM 130 0");
            }
            finally {
                client.disconnect();
            }
        }
    }

    @Test
    public void shouldStartAtScnWhenServerIsReady() throws Exception {
        final List<RedoRequest> requests = new CopyOnWriteArrayList<>();
        try (ServerSocket server = new ServerSocket(0)) {
            final Thread olr = new Thread(() -> serve(server, ResponseCode.READY, requests));
            olr.setDaemon(true);
            olr.start();

            final OlrNetworkClient client = new OlrNetworkClient(config(server));
            try {
                assertThat(client.connect(Scn.valueOf(100), Scn.valueOf(120), 3L)).isTrue();
                assertThat(requests.get(1).getCode()).isEqualTo(RequestCode.START);
                assertThat(requests.get(1).getScn()).isEqualTo(100L);
            }
            finally {
                client.disconnect();
            }
        }
    }

    private static OracleConnectorConfig config(ServerSocket server) {
        return new OracleConnectorConfig(TestHelper.defaultConfig()
                .with(OracleConnectorConfig.OLR_HOST, "127.0.0.1")
                .with(OracleConnectorConfig.OLR_PORT, server.getLocalPort())
                .with(OracleConnectorConfig.OLR_SOURCE, "S1")
                .build());
    }

    private static void serve(ServerSocket server, ResponseCode infoResponse, List<RedoRequest> requests) {
        try (Socket socket = server.accept()) {
            final DataInputStream in = new DataInputStream(socket.getInputStream());
            final OutputStream out = socket.getOutputStream();
            requests.add(readRequest(in));
            write(out, RedoResponse.newBuilder().setCode(infoResponse).build().toByteArray());
            requests.add(readRequest(in));
            write(out, RedoResponse.newBuilder().setCode(ResponseCode.REPLICATE).build().toByteArray());
            while (true) {
                requests.add(readRequest(in));
            }
        }
        catch (IOException e) {
            // the client closed the connection
        }
    }

    private static RedoRequest readRequest(InputStream in) throws IOException {
        final byte[] size = in.readNBytes(4);
        if (size.length < 4) {
            throw new IOException("closed");
        }
        final byte[] body = in.readNBytes(ByteBuffer.wrap(size).order(ByteOrder.LITTLE_ENDIAN).getInt());
        return RedoRequest.parseFrom(body);
    }

    private static void write(OutputStream out, byte[] body) throws IOException {
        out.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(body.length).array());
        out.write(body);
        out.flush();
    }
}
