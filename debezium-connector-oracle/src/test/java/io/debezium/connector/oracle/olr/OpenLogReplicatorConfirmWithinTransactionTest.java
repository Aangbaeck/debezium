/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.jdbc.OracleConnectionFactory;
import io.debezium.connector.oracle.olr.client.StreamPosition;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoRequest;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoResponse;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RequestCode;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.ResponseCode;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * With {@code openlogreplicator.confirm.within.transactions} the flushed offset's own position (SCN and index) is
 * confirmed, also inside a transaction, so OpenLogReplicator releases a transaction while it is delivered. After a
 * restart the connector continues after the position the server confirmed and discards what it already emitted.
 */
public class OpenLogReplicatorConfirmWithinTransactionTest {

    private static final String XID = "0x0001.002.00000003";
    private static final String OTHER_XID = "0x0001.002.00000004";
    /** In the messages of {@link FakeOlr}: wait for the next request before sending the rest. */
    private static final String AWAIT_REQUEST = "await-request";

    private OracleDatabaseSchema schema;
    private EventDispatcher<OraclePartition, TableId> dispatcher;
    private List<Long> dispatchedIndexes;
    private List<String> startedTransactions;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        schema = mock(OracleDatabaseSchema.class);
        final Table table = Table.editor()
                .tableId(new TableId("S1", "DEBEZIUM", "CAPTURED"))
                .addColumn(Column.editor().name("ID").jdbcType(Types.NUMERIC).type("NUMBER").position(1).create())
                .setPrimaryKeyNames("ID")
                .create();
        when(schema.tableFor(any())).thenAnswer(invocation -> {
            final TableId id = invocation.getArgument(0);
            return "CAPTURED".equals(id.table()) ? table : null;
        });
        when(schema.getLobColumnsForTable(any())).thenReturn(Collections.emptyList());

        dispatcher = mock(EventDispatcher.class);
        dispatchedIndexes = new CopyOnWriteArrayList<>();
        startedTransactions = new CopyOnWriteArrayList<>();
        doAnswer(invocation -> {
            final OpenLogReplicatorChangeRecordEmitter emitter = invocation.getArgument(2);
            dispatchedIndexes.add(((OracleOffsetContext) emitter.getOffset()).getScnIndex());
            return true;
        }).when(dispatcher).dispatchDataChangeEvent(any(), any(), any());
        doAnswer(invocation -> {
            final OracleOffsetContext offset = invocation.getArgument(2);
            final TransactionContext context = offset.getTransactionContext();
            startedTransactions.add(invocation.getArgument(1) + " restored=" + context.getTransactionId() + " total=" + context.getTotalEventCount());
            return null;
        }).when(dispatcher).dispatchTransactionStartedEvent(any(), any(String.class), any(), any());
    }

    private static OracleConnectorConfig config(int port, boolean withinTransactions, boolean completedTransactions) {
        return new OracleConnectorConfig(TestHelper.defaultConfig()
                .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "olr")
                .with(OracleConnectorConfig.OLR_SOURCE, "S1")
                .with(OracleConnectorConfig.OLR_HOST, "127.0.0.1")
                .with(OracleConnectorConfig.OLR_PORT, port)
                .with(OracleConnectorConfig.OLR_CONFIRM_WITHIN_TRANSACTIONS, withinTransactions)
                .with(OracleConnectorConfig.OLR_CONFIRM_COMPLETED_TRANSACTIONS, completedTransactions)
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CAPTURED")
                .build());
    }

    private OpenLogReplicatorStreamingChangeEventSource newSource(OracleConnectorConfig config) {
        return new OpenLogReplicatorStreamingChangeEventSource(config, mock(OracleConnectionFactory.class), dispatcher,
                mock(ErrorHandler.class), Clock.system(), schema, mock(OpenLogReplicatorStreamingChangeEventSourceMetrics.class),
                mock(SnapshotterService.class));
    }

    private static OracleOffsetContext load(OracleConnectorConfig config, Map<String, Object> offset) {
        return new OpenLogReplicatorOracleOffsetContextLoader(config).load(offset);
    }

    /** An offset stored while the transaction XID was being emitted: 3 rows of it so far, the last at index 7. */
    private static Map<String, Object> midTransactionOffset() {
        final Map<String, Object> offset = new HashMap<>();
        offset.put("scn", "100");
        offset.put("scn_idx", 7L);
        offset.put("txId", XID);
        offset.put(TransactionContext.OFFSET_TRANSACTION_ID, XID);
        offset.put("transaction_data_collection_order_S1.DEBEZIUM.CAPTURED", 3L);
        return offset;
    }

    @Test
    void shouldConfirmOffsetIndexInsideTransaction() throws Exception {
        final OracleConnectorConfig config = config(1, true, false);
        final OpenLogReplicatorStreamingChangeEventSource source = newSource(config);
        source.init(load(config, Map.of("scn", "50")));

        assertPosition(source.confirmPosition(Map.of("scn", "100", "scn_idx", 7L)), 100, 7);
        // JSON offsets may come back with an int
        assertPosition(source.confirmPosition(Map.of("scn", "100", "scn_idx", 7)), 100, 7);
        // an offset without an index (a snapshot, another adapter, a heartbeat before any change)
        assertPosition(source.confirmPosition(Map.of("scn", "100")), 100, 0);
    }

    @Test
    void shouldConfirmStartOfOffsetScnWhenDisabled() throws Exception {
        final OracleConnectorConfig config = config(1, false, false);
        final OpenLogReplicatorStreamingChangeEventSource source = newSource(config);
        source.init(load(config, Map.of("scn", "50")));

        assertPosition(source.confirmPosition(Map.of("scn", "100", "scn_idx", 7L)), 100, 0);
    }

    @Test
    void shouldConfirmFurtherOfOffsetAndCompletedPosition() throws Exception {
        final OracleConnectorConfig config = config(1, true, true);
        final OpenLogReplicatorStreamingChangeEventSource source = newSource(config);
        source.init(load(config, Map.of("scn", "50")));
        final long epoch = epochOf(source);

        // a transaction completed at (100, 9); the offset still points at its last row
        assertPosition(source.confirmPosition(Map.of("scn", "100", "scn_idx", 7L,
                OracleOffsetContext.OLR_COMPLETED_SCN_KEY, "100", OracleOffsetContext.OLR_COMPLETED_SCN_INDEX_KEY, 9L,
                OracleOffsetContext.OLR_COMPLETED_EPOCH_KEY, epoch)), 100, 9);
        // the next transaction at a later SCN is being emitted; the completed position lies before it
        assertPosition(source.confirmPosition(Map.of("scn", "120", "scn_idx", 4L,
                OracleOffsetContext.OLR_COMPLETED_SCN_KEY, "100", OracleOffsetContext.OLR_COMPLETED_SCN_INDEX_KEY, 9L,
                OracleOffsetContext.OLR_COMPLETED_EPOCH_KEY, epoch)), 120, 4);
        // a completed position of an earlier connection does not count, the offset's own position does
        assertPosition(source.confirmPosition(Map.of("scn", "100", "scn_idx", 7L,
                OracleOffsetContext.OLR_COMPLETED_SCN_KEY, "100", OracleOffsetContext.OLR_COMPLETED_SCN_INDEX_KEY, 9L,
                OracleOffsetContext.OLR_COMPLETED_EPOCH_KEY, epoch + 1)), 100, 7);
    }

    @Test
    void shouldContinueAfterOffsetNoFurtherThanServerConfirmed() throws Exception {
        final OracleConnectorConfig config = config(1, true, false);
        final OpenLogReplicatorStreamingChangeEventSource source = newSource(config);
        source.init(load(config, midTransactionOffset()));

        // nothing confirmed in the server's run (it restarted): from the start of the offset SCN
        assertPosition(source.continuePosition(Scn.valueOf(100), null), 100, 0);
        assertPosition(source.continuePosition(Scn.valueOf(100), new StreamPosition(Scn.valueOf(90), 4L)), 100, 0);
        assertPosition(source.continuePosition(Scn.valueOf(100), new StreamPosition(Scn.valueOf(100), 3L)), 100, 3);
        assertPosition(source.continuePosition(Scn.valueOf(100), new StreamPosition(Scn.valueOf(100), 7L)), 100, 7);
        // the server confirmed more than was flushed (a rewound offset): the offset, which OLR then refuses
        assertPosition(source.continuePosition(Scn.valueOf(100), new StreamPosition(Scn.valueOf(100), 9L)), 100, 7);
        assertPosition(source.continuePosition(Scn.valueOf(100), new StreamPosition(Scn.valueOf(120), 1L)), 100, 7);
    }

    @Test
    void shouldContinueAtStartOfOffsetScnWhenDisabled() throws Exception {
        final OracleConnectorConfig config = config(1, false, true);
        final OpenLogReplicatorStreamingChangeEventSource source = newSource(config);
        source.init(load(config, midTransactionOffset()));

        assertPosition(source.continuePosition(Scn.valueOf(100), new StreamPosition(Scn.valueOf(100), 3L)), 100, 0);
    }

    /**
     * The connector restarts in the middle of a transaction while the server keeps streaming and has confirmed part of
     * what was flushed: it continues after the server's position, discards the rows it already emitted, keeps the
     * transaction's restored counters, and confirms inside the transaction as offsets are flushed.
     */
    @Test
    void shouldContinueMidTransactionOnRunningServer() throws Exception {
        final List<String> messages = new ArrayList<>();
        for (int idx = 4; idx <= 9; ++idx) {
            messages.add(row(100, idx, XID));
        }
        messages.add(commit(100, 10, XID));
        messages.add(begin(120, 1, OTHER_XID));
        messages.add(row(120, 2, OTHER_XID));
        messages.add(commit(120, 3, OTHER_XID));

        try (FakeOlr olr = new FakeOlr(ResponseCode.REPLICATE, new StreamPosition(Scn.valueOf(100), 3L), messages)) {
            final Streaming streaming = stream(olr, config(olr.port(), true, false), midTransactionOffset());
            assertThat(olr.request(1).getCode()).isEqualTo(RequestCode.CONTINUE);
            assertThat(olr.request(1).getCScn()).isEqualTo(100L);
            assertThat(olr.request(1).getCIdx()).isEqualTo(3L);

            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> dispatchedIndexes.size() >= 3);
            // rows 4..7 were emitted before the restart
            assertThat(dispatchedIndexes).containsExactly(8L, 9L, 2L);
            // the start event of the restored transaction finds its counters in the offset, so the
            // transaction monitor continues them rather than starting the transaction again
            assertThat(startedTransactions).hasSize(2);
            assertThat(startedTransactions.get(0)).isEqualTo(XID + " restored=" + XID + " total=3");
            assertThat(startedTransactions.get(1)).startsWith(OTHER_XID + " ");

            // offsets flushed inside the transaction are confirmed as they come
            streaming.source.commitOffset(Map.of(), Map.of("scn", "100", "scn_idx", 8L));
            streaming.source.commitOffset(Map.of(), Map.of("scn", "100", "scn_idx", 9L));
            streaming.source.commitOffset(Map.of(), Map.of("scn", "100", "scn_idx", 9L));
            streaming.source.commitOffset(Map.of(), Map.of("scn", "120", "scn_idx", 2L));
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> olr.requests.size() >= 5);
            Thread.sleep(200);
            assertThat(olr.requests.subList(2, olr.requests.size()))
                    .extracting(r -> r.getCode() + " " + r.getCScn() + " " + r.getCIdx())
                    .containsExactly("CONFIRM 100 8", "CONFIRM 100 9", "CONFIRM 120 2");
            olr.close();
            streaming.thread.join(10_000);
        }
    }

    /**
     * The server starts anew (no writer checkpoint): it reads the transaction from its start, the connector discards
     * the rows through its offset and confirms the stored offset on the new connection, so the server can release the
     * rows it resent even before a new row is flushed.
     */
    @Test
    void shouldReplayTransactionFromStartWhenServerStarts() throws Exception {
        final List<String> messages = new ArrayList<>();
        messages.add(begin(100, 1, XID));
        for (int idx = 2; idx <= 5; ++idx) {
            messages.add(row(100, idx, XID));
        }
        // the rest of the resent transaction reaches the server's queue after the first CONFIRM
        messages.add(AWAIT_REQUEST);
        for (int idx = 6; idx <= 9; ++idx) {
            messages.add(row(100, idx, XID));
        }
        messages.add(commit(100, 10, XID));

        try (FakeOlr olr = new FakeOlr(ResponseCode.READY, null, messages)) {
            final Map<String, Object> stored = midTransactionOffset();
            final Streaming streaming = stream(olr, config(olr.port(), true, false), stored);
            assertThat(olr.request(1).getCode()).isEqualTo(RequestCode.START);
            assertThat(olr.request(1).getScn()).isEqualTo(100L);

            // rows 2..5 are discarded, then the stored offset is confirmed on the new connection
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
                streaming.source.commitOffset(Map.of(), stored);
                return olr.requests.size() >= 3;
            });
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> dispatchedIndexes.size() >= 2);
            assertThat(dispatchedIndexes).containsExactly(8L, 9L);
            assertThat(startedTransactions).containsExactly(XID + " restored=" + XID + " total=3");

            // rows 6 and 7 were discarded after it: the unchanged offset is confirmed again, once
            streaming.source.commitOffset(Map.of(), stored);
            streaming.source.commitOffset(Map.of(), stored);
            Thread.sleep(300);
            assertThat(olr.requests.subList(2, olr.requests.size()))
                    .extracting(r -> r.getCode() + " " + r.getCScn() + " " + r.getCIdx())
                    .containsExactly("CONFIRM 100 7", "CONFIRM 100 7");
            olr.close();
            streaming.thread.join(10_000);
        }
    }

    /** Without the option a transaction is never confirmed while it is emitted, only once a later SCN is. */
    @Test
    void shouldNotConfirmInsideTransactionWhenDisabled() throws Exception {
        try (FakeOlr olr = new FakeOlr(ResponseCode.REPLICATE, new StreamPosition(Scn.valueOf(90), 1L), List.of())) {
            final Streaming streaming = stream(olr, config(olr.port(), false, false), Map.of("scn", "90", "scn_idx", 1L));
            assertThat(olr.request(1).getCode()).isEqualTo(RequestCode.CONTINUE);
            assertThat(olr.request(1).getCScn()).isEqualTo(90L);
            assertThat(olr.request(1).getCIdx()).isEqualTo(0L);

            streaming.source.commitOffset(Map.of(), Map.of("scn", "95", "scn_idx", 1L));
            streaming.source.commitOffset(Map.of(), Map.of("scn", "100", "scn_idx", 2L));
            streaming.source.commitOffset(Map.of(), Map.of("scn", "100", "scn_idx", 5000L));
            streaming.source.commitOffset(Map.of(), Map.of("scn", "120", "scn_idx", 1L));
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> olr.requests.size() >= 4);
            Thread.sleep(200);
            assertThat(olr.requests.subList(2, olr.requests.size()))
                    .extracting(r -> r.getCode() + " " + r.getCScn() + " " + r.getCIdx())
                    .containsExactly("CONFIRM 100 0", "CONFIRM 120 0");
            olr.close();
            streaming.thread.join(10_000);
        }
    }

    private static long epochOf(OpenLogReplicatorStreamingChangeEventSource source) {
        // a CONTINUE stores the position with the new connection's epoch
        source.onStreamStarted(Scn.valueOf(50), new StreamPosition(Scn.valueOf(60), 1L));
        return source.getOffsetContext().getCompletedEpoch();
    }

    private record Streaming(OpenLogReplicatorStreamingChangeEventSource source, Thread thread) {
    }

    private Streaming stream(FakeOlr olr, OracleConnectorConfig config, Map<String, Object> stored) {
        final OpenLogReplicatorStreamingChangeEventSource restarted = newSource(config);
        final OracleOffsetContext loaded = load(config, new HashMap<>(stored));
        final ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true);
        final Thread thread = new Thread(() -> {
            try {
                restarted.init(loaded);
                restarted.execute(context, new OraclePartition("server1", "S1"), loaded);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        thread.setDaemon(true);
        thread.start();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> olr.requests.size() >= 2);
        return new Streaming(restarted, thread);
    }

    /**
     * Answers INFO with the given code and confirmed position, the next request with REPLICATE, then sends the given
     * messages and records every request.
     */
    private static final class FakeOlr implements AutoCloseable {
        private final ServerSocket server = new ServerSocket(0);
        private final List<RedoRequest> requests = new CopyOnWriteArrayList<>();
        private volatile Socket socket;

        FakeOlr(ResponseCode infoCode, StreamPosition confirmed, List<String> messages) throws IOException {
            final Thread thread = new Thread(() -> serve(infoCode, confirmed, messages));
            thread.setDaemon(true);
            thread.start();
        }

        int port() {
            return server.getLocalPort();
        }

        RedoRequest request(int index) {
            return requests.get(index);
        }

        private void serve(ResponseCode infoCode, StreamPosition confirmed, List<String> messages) {
            try (Socket accepted = server.accept()) {
                socket = accepted;
                final DataInputStream in = new DataInputStream(accepted.getInputStream());
                final OutputStream out = accepted.getOutputStream();
                requests.add(readRequest(in));
                final RedoResponse.Builder info = RedoResponse.newBuilder().setCode(infoCode);
                if (confirmed != null) {
                    info.setScn(100).setCScn(confirmed.scn().longValue()).setCIdx(confirmed.index());
                }
                else if (infoCode == ResponseCode.REPLICATE) {
                    info.setScn(100).setCScn(-1L).setCIdx(0);
                }
                write(out, info.build().toByteArray());
                requests.add(readRequest(in));
                write(out, RedoResponse.newBuilder().setCode(ResponseCode.REPLICATE).build().toByteArray());
                for (String message : messages) {
                    if (AWAIT_REQUEST.equals(message)) {
                        requests.add(readRequest(in));
                    }
                    else {
                        write(out, message.getBytes(StandardCharsets.UTF_8));
                    }
                }
                while (true) {
                    requests.add(readRequest(in));
                }
            }
            catch (IOException e) {
                // closed
            }
        }

        @Override
        public void close() throws IOException {
            if (socket != null) {
                socket.close();
            }
            server.close();
        }

        private static RedoRequest readRequest(InputStream in) throws IOException {
            final byte[] size = in.readNBytes(4);
            if (size.length < 4) {
                throw new IOException("closed");
            }
            return RedoRequest.parseFrom(in.readNBytes(ByteBuffer.wrap(size).order(ByteOrder.LITTLE_ENDIAN).getInt()));
        }

        private static void write(OutputStream out, byte[] body) throws IOException {
            out.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(body.length).array());
            out.write(body);
            out.flush();
        }
    }

    private static void assertPosition(StreamPosition position, long scn, long index) {
        assertThat(position.scn()).isEqualTo(Scn.valueOf(scn));
        assertThat(position.index()).isEqualTo(index);
    }

    private static String header(long scn, long index, String xid) {
        return "\"scn\":" + scn + ",\"tm\":0,\"c_scn\":" + scn + ",\"c_idx\":" + index
                + (xid != null ? ",\"xid\":\"" + xid + "\"" : "") + ",\"db\":\"S1\",";
    }

    private static String begin(long scn, long index, String xid) {
        return "{" + header(scn, index, xid) + "\"payload\":[{\"op\":\"begin\"}]}";
    }

    private static String commit(long scn, long index, String xid) {
        return "{" + header(scn, index, xid) + "\"payload\":[{\"op\":\"commit\"}]}";
    }

    private static String row(long scn, long index, String xid) {
        return "{" + header(scn, index, xid) + "\"payload\":[{\"op\":\"c\",\"schema\":{\"owner\":\"DEBEZIUM\",\"table\":\"CAPTURED\"},"
                + "\"rid\":\"AAAA\",\"after\":{\"ID\":" + index + "}}]}";
    }
}
