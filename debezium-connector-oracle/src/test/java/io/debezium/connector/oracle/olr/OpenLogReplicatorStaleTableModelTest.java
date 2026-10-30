/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.BaseChangeRecordEmitter;
import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.jdbc.OracleConnectionFactory;
import io.debezium.connector.oracle.olr.client.StreamingEvent;
import io.debezium.connector.oracle.olr.client.payloads.AbstractMutationEvent;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * A table replaced under the same name without a DDL for it, as {@code DBMS_REDEFINITION} does, is reloaded from the
 * database when a change no longer fits its model.
 */
public class OpenLogReplicatorStaleTableModelTest {

    private static final String XID = "0x0001.002.00000003";
    private static final TableId TABLE_ID = new TableId("S1", "DEBEZIUM", "CAPTURED");
    private static final String OLD_COLUMNS = "[" + number("ID", 10, 0) + "," + number("QTY", 15, 3) + "," + varchar("TXT", 40) + "]";
    private static final String NEW_COLUMNS = "[" + number("ID", 10, 0) + "," + varchar("TXT", 80) + "," + number("QTY", 18, 4) + ","
            + varchar("EXTRA", 20) + "]";

    private final ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private OracleConnectorConfig connectorConfig;
    private OracleConnection connection;
    private EventDispatcher<OraclePartition, TableId> dispatcher;
    private OpenLogReplicatorStreamingChangeEventSource source;
    private final AtomicReference<Table> model = new AtomicReference<>();
    private final AtomicReference<Table> modelInDatabase = new AtomicReference<>();
    private final List<Map<String, Object>> schemaChangeOffsets = new ArrayList<>();

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        connectorConfig = new OracleConnectorConfig(TestHelper.defaultConfig()
                .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "olr")
                .with(OracleConnectorConfig.OLR_SOURCE, "S1")
                .with(OracleConnectorConfig.OLR_HOST, "127.0.0.1")
                .with(OracleConnectorConfig.OLR_PORT, 1)
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CAPTURED")
                .with(OracleConnectorConfig.PDB_NAME, "PDB1")
                .build());

        model.set(oldTable());
        modelInDatabase.set(newTable());

        final OracleDatabaseSchema schema = mock(OracleDatabaseSchema.class);
        when(schema.tableFor(any())).thenAnswer(invocation -> TABLE_ID.equals(invocation.getArgument(0)) ? model.get() : null);
        when(schema.getLobColumnsForTable(any())).thenReturn(Collections.emptyList());
        final Tables tables = mock(Tables.class);
        when(tables.removeTable(TABLE_ID)).thenAnswer(invocation -> model.getAndSet(null));
        when(schema.getTables()).thenReturn(tables);

        connection = mock(OracleConnection.class);
        when(connection.getTableMetadataDdl(TABLE_ID)).thenReturn("CREATE TABLE ...");
        final OracleConnectionFactory connectionFactory = mock(OracleConnectionFactory.class);
        when(connectionFactory.mainConnection()).thenReturn(connection);

        // Stands in for the DDL parser: the dispatched CREATE TABLE installs the database's model, unless the
        // table is still known, as the parser ignores CREATE TABLE for a table it has.
        dispatcher = mock(EventDispatcher.class);
        doAnswer(invocation -> {
            schemaChangeOffsets.add(new HashMap<>(((OracleOffsetContext) invocation.getArgument(1)).getOffset()));
            model.compareAndSet(null, modelInDatabase.get());
            return null;
        }).when(dispatcher).dispatchSchemaChangeEvent(any(), any(), any(), any());

        source = new OpenLogReplicatorStreamingChangeEventSource(connectorConfig, connectionFactory, dispatcher,
                mock(ErrorHandler.class), Clock.system(), schema, mock(OpenLogReplicatorStreamingChangeEventSourceMetrics.class),
                mock(SnapshotterService.class));
        source.init(OracleOffsetContext.create()
                .logicalName(connectorConfig)
                .scn(Scn.valueOf(50))
                .commitScn(CommitScn.empty())
                .snapshotPendingTransactions(Collections.emptyMap())
                .transactionContext(new TransactionContext())
                .incrementalSnapshotContext(new SignalBasedIncrementalSnapshotContext<>())
                .build());
    }

    @Test
    void shouldKeepModelForChangeThatFitsIt() throws Exception {
        read(begin(100, 1));
        read(row(100, 2, OLD_COLUMNS, "{\"ID\":1,\"QTY\":\"1.500\",\"TXT\":\"before\"}"));

        verify(dispatcher, never()).dispatchSchemaChangeEvent(any(), any(), any(), any());
        verify(connection, never()).getTableMetadataDdl(any());
        assertThat(emittedValues()).containsExactly(1, "1.500", "before");
    }

    @Test
    void shouldReloadModelBeforeChangeOfReplacedTable() throws Exception {
        read(begin(100, 1));
        read(row(100, 2, OLD_COLUMNS, "{\"ID\":1,\"QTY\":\"1.500\",\"TXT\":\"before\"}"));
        read(commit(100, 3));
        read(begin(200, 1));
        read(row(200, 4, NEW_COLUMNS, "{\"ID\":1,\"TXT\":\"after\",\"QTY\":\"7.1255\",\"EXTRA\":\"set\"}"));

        final InOrder order = inOrder(dispatcher);
        order.verify(dispatcher).dispatchSchemaChangeEvent(any(), any(), eq(TABLE_ID), any());
        order.verify(dispatcher).dispatchDataChangeEvent(any(), eq(TABLE_ID), any());

        // just before the change: its transaction and commit SCN, and an index the change itself is above
        assertThat(schemaChangeOffsets).hasSize(1);
        assertThat(schemaChangeOffsets.get(0))
                .containsEntry("scn", "200")
                .containsEntry("scn_idx", 3L);

        assertThat(emittedValues()).containsExactly(1, "after", "7.1255", "set");

        // the DDL is read in the PDB, and the main connection goes back to the root container
        final InOrder session = inOrder(connection);
        session.verify(connection).setSessionToPdb("PDB1");
        session.verify(connection).getTableMetadataDdl(TABLE_ID);
        session.verify(connection).resetSessionToCdb();
    }

    @Test
    void shouldReloadModelWhenNumberScaleChanged() throws Exception {
        modelInDatabase.set(table(15, 4));
        read(begin(200, 1));
        read(row(200, 2, "[" + number("ID", 10, 0) + "," + number("QTY", 15, 4) + "," + varchar("TXT", 40) + "]",
                "{\"ID\":1,\"QTY\":\"7.1255\",\"TXT\":\"after\"}"));

        verify(dispatcher).dispatchSchemaChangeEvent(any(), any(), eq(TABLE_ID), any());
        assertThat(model.get().columnWithName("QTY").scale()).contains(4);
    }

    @Test
    void shouldFailWhenReloadedModelDoesNotFitEither() throws Exception {
        // the table changed again after the change was written
        modelInDatabase.set(oldTable());
        read(begin(200, 1));

        assertThatThrownBy(() -> read(row(200, 2, NEW_COLUMNS, "{\"ID\":1,\"TXT\":\"after\",\"QTY\":\"7.1255\",\"EXTRA\":\"set\"}")))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("reloaded from the database does not match");
        verify(dispatcher, never()).dispatchDataChangeEvent(any(), any(), any());
    }

    @Test
    void shouldNotFindMismatchForMatchingChange() throws Exception {
        assertThat(mismatch(OLD_COLUMNS, "{\"ID\":1,\"QTY\":\"1.5\",\"TXT\":\"x\"}", null)).isNull();
    }

    @Test
    void shouldIgnoreColumnsOnlyInSchema() throws Exception {
        // hidden, guard and unused columns are in the schema, never among the values
        final String columns = "[" + number("ID", 10, 0) + "," + number("QTY", 15, 3) + "," + varchar("TXT", 40) + ","
                + varchar("SYS_NC00005$", 1) + "]";
        assertThat(mismatch(columns, "{\"ID\":1,\"QTY\":\"1.5\",\"TXT\":\"x\"}", null)).isNull();
    }

    @Test
    void shouldFindValueForUnknownColumn() throws Exception {
        assertThat(mismatch(NEW_COLUMNS, "{\"ID\":1,\"QTY\":\"1.5\",\"TXT\":\"x\",\"EXTRA\":\"e\"}", null))
                .isEqualTo("column EXTRA is not in the model");
    }

    @Test
    void shouldIgnoreValueForExcludedColumn() throws Exception {
        final ColumnNameFilter excludeExtra = (catalog, schema, table, column) -> !"EXTRA".equals(column);
        assertThat(mismatch(OLD_COLUMNS, "{\"ID\":1,\"QTY\":\"1.5\",\"TXT\":\"x\",\"EXTRA\":\"e\"}", excludeExtra)).isNull();
    }

    @Test
    void shouldFindNumberPrecisionOrScaleChange() throws Exception {
        final String values = "{\"ID\":1,\"QTY\":\"1.5\",\"TXT\":\"x\"}";
        assertThat(mismatch("[" + number("ID", 10, 0) + "," + number("QTY", 15, 4) + "]", values, null))
                .isEqualTo("column QTY is NUMBER(15,4) in the change and NUMBER(15,3) in the model");
        assertThat(mismatch("[" + number("ID", 12, 0) + "]", values, null))
                .isEqualTo("column ID is NUMBER(12,0) in the change and NUMBER(10,0) in the model");
    }

    @Test
    void shouldFindNumberThatChangedType() throws Exception {
        assertThat(mismatch("[" + number("ID", 10, 0) + "," + varchar("QTY", 20) + "]", "{\"ID\":1,\"QTY\":\"a\"}", null))
                .isEqualTo("column QTY is varchar2 in the change and NUMBER in the model");
    }

    @Test
    void shouldIgnoreNumberWithoutDeclaredPrecision() throws Exception {
        final String values = "{\"ID\":1,\"QTY\":\"1.5\",\"TXT\":\"x\"}";
        // NUMBER and NUMBER(*,0) have no precision in the dictionary
        assertThat(mismatch("[" + number("QTY", -1, -127) + "," + number("ID", 0, 0) + "]", values, null)).isNull();
        // FLOAT is a NUMBER with a binary precision in the dictionary, and FLOAT in the model
        final Table table = Table.editor().tableId(TABLE_ID)
                .addColumn(Column.editor().name("F").jdbcType(Types.FLOAT).type("FLOAT").length(126).position(1).create())
                .create();
        assertThat(OpenLogReplicatorStreamingChangeEventSource.findModelMismatch(table,
                mutation("[" + number("F", 126, -127) + "]", "{\"F\":\"1.5\"}"), null)).isNull();
    }

    private String mismatch(String columns, String after, ColumnNameFilter filter) throws Exception {
        return OpenLogReplicatorStreamingChangeEventSource.findModelMismatch(oldTable(), mutation(columns, after), filter);
    }

    private AbstractMutationEvent mutation(String columns, String after) throws Exception {
        final StreamingEvent event = mapper.readValue(row(100, 2, columns, after), StreamingEvent.class);
        return (AbstractMutationEvent) event.getPayload().get(0);
    }

    private Object[] emittedValues() throws Exception {
        final ArgumentCaptor<ChangeRecordEmitter> emitter = ArgumentCaptor.forClass(ChangeRecordEmitter.class);
        verify(dispatcher, atLeastOnce()).dispatchDataChangeEvent(any(), eq(TABLE_ID), emitter.capture());
        final Field field = BaseChangeRecordEmitter.class.getDeclaredField("newColumnValues");
        field.setAccessible(true);
        return (Object[]) field.get(emitter.getAllValues().get(emitter.getAllValues().size() - 1));
    }

    private static Table oldTable() {
        return table(15, 3);
    }

    private static Table newTable() {
        return Table.editor().tableId(TABLE_ID)
                .addColumn(numberColumn("ID", 10, 0, 1))
                .addColumn(Column.editor().name("TXT").jdbcType(Types.VARCHAR).type("VARCHAR2").length(80).position(2).create())
                .addColumn(numberColumn("QTY", 18, 4, 3))
                .addColumn(Column.editor().name("EXTRA").jdbcType(Types.VARCHAR).type("VARCHAR2").length(20).position(4).create())
                .setPrimaryKeyNames("ID")
                .create();
    }

    private static Table table(int qtyPrecision, int qtyScale) {
        return Table.editor().tableId(TABLE_ID)
                .addColumn(numberColumn("ID", 10, 0, 1))
                .addColumn(numberColumn("QTY", qtyPrecision, qtyScale, 2))
                .addColumn(Column.editor().name("TXT").jdbcType(Types.VARCHAR).type("VARCHAR2").length(40).position(3).create())
                .setPrimaryKeyNames("ID")
                .create();
    }

    private static Column numberColumn(String name, int precision, int scale, int position) {
        return Column.editor().name(name).jdbcType(Types.NUMERIC).type("NUMBER").length(precision).scale(scale).position(position).create();
    }

    private static String number(String name, int precision, int scale) {
        return "{\"name\":\"" + name + "\",\"type\":\"number\",\"precision\":" + precision + ",\"scale\":" + scale + ",\"nullable\":true}";
    }

    private static String varchar(String name, int length) {
        return "{\"name\":\"" + name + "\",\"type\":\"varchar2\",\"length\":" + length + ",\"nullable\":true}";
    }

    private void read(String json) throws Exception {
        source.onEvent(mapper.readValue(json, StreamingEvent.class));
    }

    private static String header(long scn, long index) {
        return "\"scn\":" + scn + ",\"tm\":0,\"c_scn\":" + scn + ",\"c_idx\":" + index + ",\"xid\":\"" + XID + "\",\"db\":\"S1\",";
    }

    private static String begin(long scn, long index) {
        return "{" + header(scn, index) + "\"payload\":[{\"op\":\"begin\"}]}";
    }

    private static String commit(long scn, long index) {
        return "{" + header(scn, index) + "\"payload\":[{\"op\":\"commit\"}]}";
    }

    private static String row(long scn, long index, String columns, String after) {
        return "{" + header(scn, index) + "\"payload\":[{\"op\":\"c\",\"schema\":{\"owner\":\"DEBEZIUM\",\"table\":\"CAPTURED\",\"obj\":7,"
                + "\"columns\":" + columns + "},\"rid\":\"AAAA\",\"after\":" + after + "}]}";
    }
}
