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

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import io.debezium.connector.oracle.olr.OpenLogReplicatorStreamingChangeEventSource.Position;
import io.debezium.connector.oracle.olr.client.StreamingEvent;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
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
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        connectorConfig = new OracleConnectorConfig(TestHelper.defaultConfig()
                .with(OracleConnectorConfig.OLR_SOURCE, "S1")
                .with(OracleConnectorConfig.OLR_HOST, "127.0.0.1")
                .with(OracleConnectorConfig.OLR_PORT, 1)
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CAPTURED")
                .build());
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

        source = new OpenLogReplicatorStreamingChangeEventSource(connectorConfig, mock(OracleConnectionFactory.class), dispatcher,
                mock(ErrorHandler.class), Clock.system(), schema, mock(OpenLogReplicatorStreamingChangeEventSourceMetrics.class),
                mock(SnapshotterService.class));
        offsetContext = OracleOffsetContext.create()
                .logicalName(connectorConfig)
                .scn(Scn.valueOf(50))
                .commitScn(CommitScn.empty())
                .snapshotPendingTransactions(Collections.emptyMap())
                .transactionContext(new TransactionContext())
                .incrementalSnapshotContext(new SignalBasedIncrementalSnapshotContext<>())
                .build();
        source.init(offsetContext);
        source.onStreamStarted(false);
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
        assertPosition(Position.resumeFrom(Scn.valueOf(100), null, null), 100, 0);
        assertPosition(Position.resumeFrom(Scn.valueOf(100), Scn.valueOf(100), 4L), 100, 4);
        assertPosition(Position.resumeFrom(Scn.valueOf(100), Scn.valueOf(120), 1L), 120, 1);
        // a transaction at a later SCN is open in the stream: its start is before the completed position of the next SCN
        assertPosition(Position.resumeFrom(Scn.valueOf(130), Scn.valueOf(120), 1L), 130, 0);
        assertPosition(Position.resumeFrom(Scn.valueOf(100), Scn.valueOf(100), 0L), 100, 0);
        assertPosition(Position.resumeFrom(Scn.valueOf(100), Scn.NULL, 7L), 100, 0);
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
        source.onStreamStarted(false);
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
    void shouldKeepCompletedPositionWhenContinuing() throws Exception {
        offsetContext.setScn(Scn.valueOf(100));
        offsetContext.setCompletedPosition(Scn.valueOf(120), 2L, 77L);
        final Map<String, Object> offset = new HashMap<>(offsetContext.getOffset());

        source.init(load(offset));
        source.onStreamStarted(true);
        assertPosition(source.confirmPosition(offset), 120, 2);
    }

    private OracleOffsetContext load(Map<String, Object> offset) {
        return new OpenLogReplicatorOracleOffsetContextLoader(connectorConfig).load(offset);
    }

    private void assertCompleted(long scn, long index) {
        assertThat(offsetContext.getCompletedScn()).isEqualTo(Scn.valueOf(scn));
        assertThat(offsetContext.getCompletedScnIndex()).isEqualTo(index);
    }

    private static void assertPosition(Position position, long scn, long index) {
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
