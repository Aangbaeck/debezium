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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.jdbc.OracleConnectionFactory;
import io.debezium.connector.oracle.olr.client.StreamPosition;
import io.debezium.connector.oracle.olr.client.StreamingEvent;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoRequest;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoResponse;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RequestCode;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.ResponseCode;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * The position through which the OpenLogReplicator stream has been emitted completely: it moves on a commit and on a
 * checkpoint read outside a transaction, never backwards, and never moves the offset SCN.
 */
public class OpenLogReplicatorCompletedPositionTest {

    private static final String XID = "0x0001.002.00000003";
    private static final String OTHER_XID = "0x0001.002.00000004";

    private final ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private OracleConnectorConfig connectorConfig;
    private OracleDatabaseSchema schema;
    private EventDispatcher<OraclePartition, TableId> dispatcher;
    private OpenLogReplicatorStreamingChangeEventSource source;
    private OracleOffsetContext offsetContext;
    private List<Map<String, Object>> heartbeatOffsets;
    private List<Map<String, Object>> commitOffsets;

    @BeforeEach
    void setUp() throws Exception {
        setUp(true);
    }

    @SuppressWarnings("unchecked")
    private void setUp(boolean confirmCompletedTransactions) throws Exception {
        connectorConfig = config(1, confirmCompletedTransactions);
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
        heartbeatOffsets = new ArrayList<>();
        commitOffsets = new ArrayList<>();
        doAnswer(invocation -> heartbeatOffsets.add(new HashMap<>(((OracleOffsetContext) invocation.getArgument(1)).getOffset())))
                .when(dispatcher).alwaysDispatchHeartbeatEvent(any(), any());
        doAnswer(invocation -> commitOffsets.add(new HashMap<>(((OracleOffsetContext) invocation.getArgument(1)).getOffset())))
                .when(dispatcher).dispatchTransactionCommittedEvent(any(), any(), any());

        source = newSource();
        offsetContext = OracleOffsetContext.create()
                .logicalName(connectorConfig)
                .scn(Scn.valueOf(50))
                .commitScn(CommitScn.empty())
                .snapshotPendingTransactions(Collections.emptyMap())
                .transactionContext(new TransactionContext())
                .incrementalSnapshotContext(new SignalBasedIncrementalSnapshotContext<>())
                .build();
        source.init(offsetContext);
        if (confirmCompletedTransactions) {
            source.onStreamStarted(Scn.valueOf(50), null);
        }
    }

    private static OracleConnectorConfig config(int port, boolean confirmCompletedTransactions) {
        return new OracleConnectorConfig(TestHelper.defaultConfig()
                .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "olr")
                .with(OracleConnectorConfig.OLR_SOURCE, "S1")
                .with(OracleConnectorConfig.OLR_HOST, "127.0.0.1")
                .with(OracleConnectorConfig.OLR_PORT, port)
                .with(OracleConnectorConfig.OLR_CONFIRM_COMPLETED_TRANSACTIONS, confirmCompletedTransactions)
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CAPTURED")
                .build());
    }

    private OpenLogReplicatorStreamingChangeEventSource newSource() {
        return newSource(connectorConfig);
    }

    private OpenLogReplicatorStreamingChangeEventSource newSource(OracleConnectorConfig config) {
        return new OpenLogReplicatorStreamingChangeEventSource(config, mock(OracleConnectionFactory.class), dispatcher,
                mock(ErrorHandler.class), Clock.system(), schema, mock(OpenLogReplicatorStreamingChangeEventSourceMetrics.class),
                mock(SnapshotterService.class));
    }

    @Test
    void shouldCompleteThroughCommitWithoutMovingOffsetScn() throws Exception {
        read(begin(100, 1, XID));
        read(row("CAPTURED", 100, 2, XID));
        read(row("CAPTURED", 100, 3, XID));
        assertThat(offsetContext.getCompletedScn()).isNull();
        read(commit(100, 4, XID));

        assertThat(offsetContext.getScn()).isEqualTo(Scn.valueOf(100));
        assertThat(offsetContext.getScnIndex()).isEqualTo(3L);
        assertThat(offsetContext.getTransactionId()).isEqualTo(XID);
        assertCompleted(100, 4);

        // the records sent for the commit carry the completed position
        assertThat(heartbeatOffsets).hasSize(1);
        assertThat(heartbeatOffsets.get(0)).containsEntry(OracleOffsetContext.OLR_COMPLETED_SCN_KEY, "100")
                .containsEntry(OracleOffsetContext.OLR_COMPLETED_SCN_INDEX_KEY, 4L);
        assertThat(commitOffsets).hasSize(1);
        assertThat(commitOffsets.get(0)).containsEntry(OracleOffsetContext.OLR_COMPLETED_SCN_INDEX_KEY, 4L);
    }

    @Test
    void shouldCompleteThroughTransactionWithoutCapturedChanges() throws Exception {
        read(begin(100, 1, XID));
        read(row("OTHER", 100, 2, XID));
        read(commit(100, 3, XID));

        // the offset SCN only moves with emitted changes, the completed position with every commit
        assertThat(offsetContext.getScn()).isEqualTo(Scn.valueOf(50));
        assertThat(offsetContext.getScnIndex()).isNull();
        assertCompleted(100, 3);
    }

    @Test
    void shouldCompleteThroughCheckpointOutsideTransactionOnly() throws Exception {
        read(begin(100, 1, XID));
        read(row("CAPTURED", 100, 2, XID));
        read(commit(100, 3, XID));
        read(checkpoint(120, 1));
        assertCompleted(120, 1);
        assertThat(offsetContext.getScn()).isEqualTo(Scn.valueOf(100));

        read(begin(130, 1, OTHER_XID));
        read(row("CAPTURED", 130, 2, OTHER_XID));
        read(checkpoint(140, 1));
        assertCompleted(120, 1);

        read(commit(130, 3, OTHER_XID));
        assertCompleted(130, 3);
    }

    @Test
    void shouldNotMoveCompletedPositionBack() throws Exception {
        read(begin(100, 1, XID));
        read(commit(100, 5, XID));
        read(checkpoint(100, 5));
        read(checkpoint(90, 1));
        assertCompleted(100, 5);

        // a transaction read again after a restart from an earlier SCN
        read(begin(100, 1, OTHER_XID));
        read(commit(100, 2, OTHER_XID));
        assertCompleted(100, 5);
    }

    @Test
    void shouldResumeFromCompletedPositionOnlyWhenFurtherAlong() {
        assertPosition(StreamPosition.resumeFrom(Scn.valueOf(100), null, null), 100, 0);
        assertPosition(StreamPosition.resumeFrom(Scn.valueOf(100), Scn.valueOf(100), 4L), 100, 4);
        assertPosition(StreamPosition.resumeFrom(Scn.valueOf(100), Scn.valueOf(120), 1L), 120, 1);
        // a transaction at a later SCN is open in the stream: its start is before the completed position of the next SCN
        assertPosition(StreamPosition.resumeFrom(Scn.valueOf(130), Scn.valueOf(120), 1L), 130, 0);
        assertPosition(StreamPosition.resumeFrom(Scn.valueOf(100), Scn.valueOf(100), 0L), 100, 0);
        assertPosition(StreamPosition.resumeFrom(Scn.valueOf(100), Scn.NULL, 7L), 100, 0);
    }

    @Test
    void shouldStoreAndLoadCompletedPosition() {
        offsetContext.setScn(Scn.valueOf(100));
        offsetContext.setScnIndex(3L);
        offsetContext.setCompletedPosition(Scn.valueOf(120), 2L, 77L);
        final Map<String, Object> offset = new HashMap<>(offsetContext.getOffset());
        assertThat(offset).containsEntry(OracleOffsetContext.OLR_COMPLETED_SCN_KEY, "120")
                .containsEntry(OracleOffsetContext.OLR_COMPLETED_SCN_INDEX_KEY, 2L)
                .containsEntry(OracleOffsetContext.OLR_COMPLETED_BASE_SCN_KEY, "100")
                .containsEntry(OracleOffsetContext.OLR_COMPLETED_EPOCH_KEY, 77L);

        final OracleOffsetContext loaded = load(offset);
        assertThat(loaded.getCompletedScn()).isEqualTo(Scn.valueOf(120));
        assertThat(loaded.getCompletedScnIndex()).isEqualTo(2L);
        assertThat(loaded.getCompletedBaseScn()).isEqualTo(Scn.valueOf(100));
        assertThat(loaded.getCompletedEpoch()).isEqualTo(77L);

        // JSON offsets may come back with an int
        final Map<String, Object> narrowed = new HashMap<>(offset);
        narrowed.put(OracleOffsetContext.OLR_COMPLETED_SCN_INDEX_KEY, 2);
        assertThat(load(narrowed).getCompletedScnIndex()).isEqualTo(2L);

        // the offset SCN moved on after the position was set
        final Map<String, Object> later = new HashMap<>(offset);
        later.put("scn", "130");
        assertThat(load(later).getCompletedScn()).isEqualTo(Scn.valueOf(120));

        // an offset written before the position existed, or by another adapter
        final Map<String, Object> legacy = new HashMap<>(offset);
        legacy.remove(OracleOffsetContext.OLR_COMPLETED_SCN_KEY);
        legacy.remove(OracleOffsetContext.OLR_COMPLETED_SCN_INDEX_KEY);
        final OracleOffsetContext legacyLoaded = load(legacy);
        assertThat(legacyLoaded.getCompletedScn()).isNull();
        assertThat(legacyLoaded.getOffset()).doesNotContainKeys(OracleOffsetContext.OLR_COMPLETED_SCN_KEY,
                OracleOffsetContext.OLR_COMPLETED_SCN_INDEX_KEY, OracleOffsetContext.OLR_COMPLETED_BASE_SCN_KEY,
                OracleOffsetContext.OLR_COMPLETED_EPOCH_KEY);
    }

    @Test
    void shouldDropCompletedPositionOfRewoundOffset() {
        offsetContext.setScn(Scn.valueOf(100));
        offsetContext.setCompletedPosition(Scn.valueOf(120), 2L, 77L);
        final Map<String, Object> offset = new HashMap<>(offsetContext.getOffset());

        // an operator set the offset SCN back and left the completed position in place
        final Map<String, Object> rewound = new HashMap<>(offset);
        rewound.put("scn", "60");
        assertThat(load(rewound).getCompletedScn()).isNull();
        assertThat(load(rewound).getOffset()).doesNotContainKey(OracleOffsetContext.OLR_COMPLETED_SCN_KEY);

        // without a base SCN the position cannot be checked
        final Map<String, Object> noBase = new HashMap<>(offset);
        noBase.remove(OracleOffsetContext.OLR_COMPLETED_BASE_SCN_KEY);
        assertThat(load(noBase).getCompletedScn()).isNull();
    }

    @Test
    void shouldIgnoreCompletedPositionOfEarlierStreamAfterStart() throws Exception {
        // a completed position from before a restart, loaded with the offset
        offsetContext.setScn(Scn.valueOf(100));
        offsetContext.setCompletedPosition(Scn.valueOf(120), 2L, 77L);
        final Map<String, Object> stale = new HashMap<>(offsetContext.getOffset());

        // the server was started again: it numbers the stream anew
        source.onStreamStarted(Scn.valueOf(100), null);
        assertThat(offsetContext.getCompletedScn()).isNull();
        assertPosition(source.confirmPosition(stale), 100, 0);

        read(begin(100, 1, XID));
        read(commit(100, 2, XID));
        assertCompleted(100, 2);
        assertPosition(source.confirmPosition(new HashMap<>(offsetContext.getOffset())), 100, 2);
        // the stale offset still does not count once the new stream completed a position
        assertPosition(source.confirmPosition(stale), 100, 0);
    }

    @Test
    void shouldKeepCompletedPositionWhenContinuingWhereServerConfirmedIt() throws Exception {
        offsetContext.setScn(Scn.valueOf(100));
        offsetContext.setCompletedPosition(Scn.valueOf(120), 2L, 77L);
        final Map<String, Object> offset = new HashMap<>(offsetContext.getOffset());

        source.init(load(offset));
        final StreamPosition continuedAfter = source.continuePosition(Scn.valueOf(100), new StreamPosition(Scn.valueOf(120), 2L));
        assertPosition(continuedAfter, 120, 2);
        source.onStreamStarted(Scn.valueOf(100), continuedAfter);
        assertThat(source.getOffsetContext().getCompletedScn()).isEqualTo(Scn.valueOf(120));
        assertThat(source.getOffsetContext().getCompletedScnIndex()).isEqualTo(2L);

        // the stored offset is confirmed as the start of its SCN until this connection stores its own
        assertPosition(source.confirmPosition(offset), 100, 0);
        assertPosition(source.confirmPosition(new HashMap<>(source.getOffsetContext().getOffset())), 120, 2);
    }

    @Test
    void shouldContinueNoFurtherThanServerConfirmed() throws Exception {
        offsetContext.setScn(Scn.valueOf(100));
        offsetContext.setCompletedPosition(Scn.valueOf(120), 2L, 77L);
        source.init(load(new HashMap<>(offsetContext.getOffset())));

        assertPosition(source.continuePosition(Scn.valueOf(100), null), 100, 0);
        assertPosition(source.continuePosition(Scn.valueOf(100), new StreamPosition(Scn.valueOf(90), 4L)), 100, 0);
        assertPosition(source.continuePosition(Scn.valueOf(100), new StreamPosition(Scn.valueOf(100), 0L)), 100, 0);
        assertPosition(source.continuePosition(Scn.valueOf(100), new StreamPosition(Scn.valueOf(110), 7L)), 110, 7);
        assertPosition(source.continuePosition(Scn.valueOf(100), new StreamPosition(Scn.valueOf(120), 1L)), 120, 1);
        assertPosition(source.continuePosition(Scn.valueOf(100), new StreamPosition(Scn.valueOf(120), 3L)), 120, 2);
        assertPosition(source.continuePosition(Scn.valueOf(100), new StreamPosition(Scn.valueOf(200), 1L)), 120, 2);

        // nothing completed: the start of the offset SCN, whatever the server confirmed
        source.init(load(Map.of("scn", "100")));
        assertPosition(source.continuePosition(Scn.valueOf(100), new StreamPosition(Scn.valueOf(200), 1L)), 100, 0);
    }

    @Test
    void shouldBehaveAsBeforeWhenDisabled() throws Exception {
        setUp(false);
        read(begin(100, 1, XID));
        read(row("CAPTURED", 100, 2, XID));
        read(commit(100, 3, XID));
        read(checkpoint(120, 1));

        assertThat(offsetContext.getCompletedScn()).isNull();
        assertThat(heartbeatOffsets).isNotEmpty().allSatisfy(offset -> assertThat(offset).doesNotContainKeys(
                OracleOffsetContext.OLR_COMPLETED_SCN_KEY, OracleOffsetContext.OLR_COMPLETED_SCN_INDEX_KEY,
                OracleOffsetContext.OLR_COMPLETED_BASE_SCN_KEY, OracleOffsetContext.OLR_COMPLETED_EPOCH_KEY));

        // an offset stored with the option on loads and confirms as one without the keys
        final Map<String, Object> stored = new HashMap<>(Map.of("scn", "100", "scn_idx", 2L,
                OracleOffsetContext.OLR_COMPLETED_SCN_KEY, "120", OracleOffsetContext.OLR_COMPLETED_SCN_INDEX_KEY, 1L,
                OracleOffsetContext.OLR_COMPLETED_BASE_SCN_KEY, "100", OracleOffsetContext.OLR_COMPLETED_EPOCH_KEY, 77L));
        final OracleOffsetContext loaded = load(stored);
        assertThat(loaded.getCompletedScn()).isNull();
        assertThat(loaded.getOffset()).doesNotContainKeys(OracleOffsetContext.OLR_COMPLETED_SCN_KEY,
                OracleOffsetContext.OLR_COMPLETED_SCN_INDEX_KEY, OracleOffsetContext.OLR_COMPLETED_BASE_SCN_KEY,
                OracleOffsetContext.OLR_COMPLETED_EPOCH_KEY);
        assertPosition(source.confirmPosition(stored), 100, 0);
    }

    /**
     * The server is started again without its writer checkpoint, and no record is flushed before the
     * connector restarts: the stored completed position belongs to the server's earlier run, and is
     * neither continued after nor confirmed.
     */
    @Test
    void shouldNotUseStoredCompletedPositionAfterUnflushedStart() throws Exception {
        // the stream is complete through (120, 3), stored with the heartbeat of that commit
        read(begin(100, 1, XID));
        read(row("CAPTURED", 100, 2, XID));
        read(commit(100, 3, XID));
        read(begin(120, 1, OTHER_XID));
        read(row("OTHER", 120, 2, OTHER_XID));
        read(commit(120, 3, OTHER_XID));
        assertCompleted(120, 3);
        final Map<String, Object> stored = heartbeatOffsets.get(heartbeatOffsets.size() - 1);
        assertThat(stored).containsEntry("scn", "100").containsEntry(OracleOffsetContext.OLR_COMPLETED_SCN_KEY, "120");

        // the server starts again and the database is quiet, so nothing is flushed
        try (FakeOlr olr = new FakeOlr(ResponseCode.READY, null)) {
            final Streaming streaming = stream(olr, stored);
            assertThat(olr.request(1).getCode()).isEqualTo(RequestCode.START);
            assertThat(olr.request(1).getScn()).isEqualTo(100L);
            assertThat(streaming.offsetContext.getCompletedScn()).isNull();
            assertThat(confirmStored(olr, streaming, stored)).isEqualTo("CONFIRM 100 0");
            olr.close();
            streaming.thread.join(10_000);
        }

        // the connector restarts with the same stored offset; the server is streaming and has
        // the start of SCN 100 confirmed, or nothing
        for (StreamPosition serverConfirmed : new StreamPosition[]{ new StreamPosition(Scn.valueOf(100), 0L), null }) {
            try (FakeOlr olr = new FakeOlr(ResponseCode.REPLICATE, serverConfirmed)) {
                final Streaming streaming = stream(olr, stored);
                assertThat(olr.request(1).getCode()).isEqualTo(RequestCode.CONTINUE);
                assertThat(olr.request(1).getCScn()).isEqualTo(100L);
                assertThat(olr.request(1).getCIdx()).isEqualTo(0L);
                assertThat(streaming.offsetContext.getCompletedScn()).isNull();
                assertThat(confirmStored(olr, streaming, stored)).isEqualTo("CONFIRM 100 0");
                olr.close();
                streaming.thread.join(10_000);
            }
        }
    }

    /**
     * The connector restarts while the server keeps streaming, so the server reports the position the
     * connector confirmed in that run.
     */
    @Test
    void shouldContinueAfterServerConfirmedPositionWhenOnlyConnectorRestarts() throws Exception {
        read(begin(100, 1, XID));
        read(row("CAPTURED", 100, 2, XID));
        read(commit(100, 3, XID));
        read(begin(120, 1, OTHER_XID));
        read(commit(120, 3, OTHER_XID));
        final Map<String, Object> stored = heartbeatOffsets.get(heartbeatOffsets.size() - 1);

        try (FakeOlr olr = new FakeOlr(ResponseCode.REPLICATE, new StreamPosition(Scn.valueOf(120), 3L))) {
            final Streaming streaming = stream(olr, stored);
            assertThat(olr.request(1).getCode()).isEqualTo(RequestCode.CONTINUE);
            assertThat(olr.request(1).getCScn()).isEqualTo(120L);
            assertThat(olr.request(1).getCIdx()).isEqualTo(3L);
            assertThat(streaming.offsetContext.getCompletedScn()).isEqualTo(Scn.valueOf(120));
            // the stored offset carries an earlier connection's epoch
            assertThat(confirmStored(olr, streaming, stored)).isEqualTo("CONFIRM 100 0");
            olr.close();
            streaming.thread.join(10_000);
        }
    }

    private record Streaming(OpenLogReplicatorStreamingChangeEventSource source, OracleOffsetContext offsetContext, Thread thread) {
    }

    private Streaming stream(FakeOlr olr, Map<String, Object> stored) {
        final OpenLogReplicatorStreamingChangeEventSource restarted = newSource(config(olr.port(), true));
        final OracleOffsetContext loaded = load(stored);
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
        return new Streaming(restarted, loaded, thread);
    }

    /** Commits the stored offset until the streaming source has connected and confirmed it. */
    private static String confirmStored(FakeOlr olr, Streaming streaming, Map<String, Object> stored) {
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
            streaming.source.commitOffset(Map.of(), stored);
            return olr.requests.size() >= 3;
        });
        final RedoRequest confirm = olr.request(2);
        return confirm.getCode() + " " + confirm.getCScn() + " " + confirm.getCIdx();
    }

    /**
     * Answers INFO with the given code and confirmed position, the next request with REPLICATE, and
     * records every request.
     */
    private static final class FakeOlr implements AutoCloseable {
        private final ServerSocket server = new ServerSocket(0);
        private final List<RedoRequest> requests = new CopyOnWriteArrayList<>();
        private volatile Socket socket;

        FakeOlr(ResponseCode infoCode, StreamPosition confirmed) throws IOException {
            final Thread thread = new Thread(() -> serve(infoCode, confirmed));
            thread.setDaemon(true);
            thread.start();
        }

        int port() {
            return server.getLocalPort();
        }

        RedoRequest request(int index) {
            return requests.get(index);
        }

        private void serve(ResponseCode infoCode, StreamPosition confirmed) {
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
                    // nothing confirmed in this run
                    info.setScn(100).setCScn(-1L).setCIdx(0);
                }
                write(out, info.build().toByteArray());
                requests.add(readRequest(in));
                write(out, RedoResponse.newBuilder().setCode(ResponseCode.REPLICATE).build().toByteArray());
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

    private OracleOffsetContext load(Map<String, Object> offset) {
        return new OpenLogReplicatorOracleOffsetContextLoader(connectorConfig).load(offset);
    }

    private void assertCompleted(long scn, long index) {
        assertThat(offsetContext.getCompletedScn()).isEqualTo(Scn.valueOf(scn));
        assertThat(offsetContext.getCompletedScnIndex()).isEqualTo(index);
    }

    private static void assertPosition(StreamPosition position, long scn, long index) {
        assertThat(position.scn()).isEqualTo(Scn.valueOf(scn));
        assertThat(position.index()).isEqualTo(index);
    }

    private void read(String json) throws Exception {
        source.onEvent(mapper.readValue(json, StreamingEvent.class));
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

    private static String checkpoint(long scn, long index) {
        return "{" + header(scn, index, null) + "\"payload\":[{\"op\":\"chkpt\",\"seq\":1,\"offset\":0}]}";
    }

    private static String row(String table, long scn, long index, String xid) {
        return "{" + header(scn, index, xid) + "\"payload\":[{\"op\":\"c\",\"schema\":{\"owner\":\"DEBEZIUM\",\"table\":\"" + table
                + "\"},\"rid\":\"AAAA\",\"after\":{\"ID\":" + index + "}}]}";
    }
}
