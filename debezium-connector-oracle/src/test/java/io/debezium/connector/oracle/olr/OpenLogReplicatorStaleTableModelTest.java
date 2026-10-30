/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.BaseChangeRecordEmitter;
import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnection.NonRelationalTableException;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleDefaultValueConverter;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.StreamingAdapter;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.jdbc.OracleConnectionFactory;
import io.debezium.connector.oracle.olr.client.StreamingEvent;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.MemorySchemaHistory;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.util.Clock;

import oracle.sql.CharacterSet;

/**
 * A table replaced under the same name without a DDL for it, as {@code DBMS_REDEFINITION} does, is reloaded when a
 * change no longer fits its model. The reloaded model follows the change's column metadata when the table changed
 * again, or was dropped, before the connector reached the change.
 */
public class OpenLogReplicatorStaleTableModelTest {

    private static final String XID = "0x0001.002.00000003";
    private static final TableId TABLE_ID = new TableId("S1", "DEBEZIUM", "CAPTURED");

    private static final String OLD_DDL = ddl("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"QTY\" NUMBER(15,3), \"TXT\" VARCHAR2(40)");
    private static final String NEW_DDL = ddl("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"TXT\" VARCHAR2(80), \"QTY\" NUMBER(18,4), \"EXTRA\" VARCHAR2(20)");

    private static final String OLD_COLUMNS = "[" + id() + "," + number("QTY", 15, 3) + "," + varchar("TXT", 40) + "]";
    private static final String NEW_COLUMNS = "[" + id() + "," + varchar("TXT", 80) + "," + number("QTY", 18, 4) + "," + varchar("EXTRA", 20) + "]";
    private static final String OLD_ROW = "{\"ID\":1,\"QTY\":\"1.500\",\"TXT\":\"before\"}";
    private static final String NEW_ROW = "{\"ID\":1,\"TXT\":\"after\",\"QTY\":\"7.1255\",\"EXTRA\":\"set\"}";
    /** The new layout with the key column renamed to ID2. */
    private static final String RENAMED_KEY_COLUMNS = "[" + number("QTY", 18, 4) + "," + varchar("TXT", 80) + ","
            + "{\"name\":\"ID2\",\"type\":\"number\",\"precision\":10,\"scale\":0,\"nullable\":false}," + varchar("EXTRA", 20) + "]";
    private static final String RENAMED_KEY_ROW = "{\"QTY\":\"7.1255\",\"TXT\":\"after\",\"ID2\":1,\"EXTRA\":\"set\"}";

    private final ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private OracleConnectorConfig connectorConfig;
    private OracleConnection connection;
    private OracleConnectionFactory connectionFactory;
    private OracleDatabaseSchema schema;
    private OraclePartition partition;
    private EventDispatcher<OraclePartition, TableId> dispatcher;
    private OpenLogReplicatorStreamingChangeEventSource source;
    private final List<SchemaChangeEvent> schemaChanges = new ArrayList<>();
    private final List<Map<String, Object>> schemaChangeOffsets = new ArrayList<>();

    @AfterEach
    void tearDown() {
        if (schema != null) {
            schema.close();
        }
    }

    private String snapshotDdl = OLD_DDL;

    private void start(String databaseDdl) throws Exception {
        start(databaseDdl, builder -> builder.with(OracleConnectorConfig.PDB_NAME, "PDB1"));
    }

    @SuppressWarnings("unchecked")
    private void start(String databaseDdl, UnaryOperator<Configuration.Builder> config) throws Exception {
        connectorConfig = new OracleConnectorConfig(config.apply(TestHelper.defaultConfig()
                .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "olr")
                .with(OracleConnectorConfig.OLR_SOURCE, "S1")
                .with(OracleConnectorConfig.OLR_HOST, "127.0.0.1")
                .with(OracleConnectorConfig.OLR_PORT, 1)
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CAPTURED")
                .with(OracleConnectorConfig.SCHEMA_HISTORY, MemorySchemaHistory.class)).build());

        connection = mock(OracleConnection.class);
        when(connection.getNationalCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.AL16UTF16_CHARSET));
        when(connection.getDatabaseCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.AL32UTF8_CHARSET));
        when(connection.getTableMetadataDdl(TABLE_ID)).thenReturn(databaseDdl);
        connectionFactory = mock(OracleConnectionFactory.class);
        when(connectionFactory.mainConnection()).thenReturn(connection);

        schema = createSchema();
        partition = new OraclePartition(connectorConfig.getLogicalName(), "S1");
        // the snapshot's model
        final OracleOffsetContext snapshotOffset = offset(50, null, null);
        snapshotOffset.tableEvent(TABLE_ID, Instant.now());
        schema.applySchemaChange(SchemaChangeEvent.ofSnapshotCreate(partition, snapshotOffset, TABLE_ID.catalog(), parse(snapshotDdl)));

        // Stands in for the dispatcher: the schema change is applied to the schema and its history.
        dispatcher = mock(EventDispatcher.class);
        doAnswer(invocation -> {
            final OracleOffsetContext offsetContext = invocation.getArgument(1);
            ((SchemaChangeEventEmitter) invocation.getArgument(3)).emitSchemaChangeEvent(event -> {
                schemaChanges.add(event);
                schemaChangeOffsets.add(new HashMap<String, Object>(offsetContext.getOffset()));
                schema.applySchemaChange(event);
            });
            return null;
        }).when(dispatcher).dispatchSchemaChangeEvent(any(), any(), any(), any());

        source = newSource(offset(50, null, null));
    }

    private OpenLogReplicatorStreamingChangeEventSource newSource(OracleOffsetContext offsetContext) throws Exception {
        final OpenLogReplicatorStreamingChangeEventSource newSource = new OpenLogReplicatorStreamingChangeEventSource(connectorConfig, connectionFactory,
                dispatcher, mock(ErrorHandler.class), Clock.system(), schema, mock(OpenLogReplicatorStreamingChangeEventSourceMetrics.class),
                mock(SnapshotterService.class));
        newSource.init(offsetContext);
        setField(newSource, "partition", partition);
        return newSource;
    }

    @Test
    void shouldDoNoExtraWorkForChangeThatFitsModel() throws Exception {
        start(NEW_DDL);
        read(begin(100, 1));
        read(row(100, 2, OLD_COLUMNS, OLD_ROW));
        read(row(100, 3, "[" + id() + "," + number("QTY", 15, 3) + "," + varchar("TXT", 40) + "," + varchar("SYS_NC00004$", 1) + "]", OLD_ROW));

        verify(connectionFactory, never()).mainConnection();
        verify(dispatcher, never()).dispatchSchemaChangeEvent(any(), any(), any(), any());
        assertThat(emittedValues()).containsExactly(1, "1.500", "before");
    }

    @Test
    void shouldReloadModelBeforeChangeOfReplacedTable() throws Exception {
        start(NEW_DDL);
        read(begin(100, 1));
        read(row(100, 2, OLD_COLUMNS, OLD_ROW));
        read(commit(100, 3));
        read(begin(200, 1));
        read(row(200, 4, NEW_COLUMNS, NEW_ROW));

        final InOrder order = inOrder(dispatcher);
        order.verify(dispatcher).dispatchSchemaChangeEvent(any(), any(), eq(TABLE_ID), any());
        order.verify(dispatcher).dispatchDataChangeEvent(any(), eq(TABLE_ID), any());

        // the database's DDL, just before the change: its transaction and commit SCN, and an index the change is above
        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0).getDdl()).isEqualTo(NEW_DDL);
        assertThat(schemaChangeOffsets.get(0)).containsEntry("scn", "200").containsEntry("scn_idx", 3L);
        assertThat(emittedValues()).containsExactly(1, "after", "7.1255", "set");
        assertThat(schema.tableFor(TABLE_ID)).isEqualTo(parse(NEW_DDL));

        // the DDL is read in the PDB, and the main connection goes back to the root container
        final InOrder session = inOrder(connection);
        session.verify(connection).setSessionToPdb("PDB1");
        session.verify(connection).getTableMetadataDdl(TABLE_ID);
        session.verify(connection).resetSessionToCdb();
    }

    @Test
    void shouldReloadModelWhenDateBecomesTimestamp() throws Exception {
        // NS is NUMBER(*,2): the snapshot has its scale, OracleDdlParser parses the database's DDL with scale 0
        snapshotDdl = ddl("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"TS\" DATE, \"TXT\" VARCHAR2(10), \"NS\" NUMBER(38,2)");
        final String newDdl = ddl("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"TS\" TIMESTAMP (6), \"TXT\" VARCHAR2(10), \"NS\" NUMBER(*,2)");
        start(newDdl);
        assertThat(schema.schemaFor(TABLE_ID).valueSchema().field("TS").schema().name()).isEqualTo(Timestamp.SCHEMA_NAME);

        read(begin(100, 1));
        read(row(100, 2, "[" + id() + "," + column("TS", "\"type\":\"date\"") + "," + varchar("TXT", 10) + "," + number("NS", -1, 2) + "]",
                "{\"ID\":1,\"TS\":1704067200000000000,\"TXT\":\"before\",\"NS\":\"1.25\"}"));
        verify(dispatcher, never()).dispatchSchemaChangeEvent(any(), any(), any(), any());

        final String newColumns = "[" + id() + "," + column("TS", "\"type\":\"timestamp\",\"length\":11") + "," + varchar("TXT", 10) + ","
                + number("NS", -1, 2) + "]";
        read(row(100, 3, newColumns, "{\"ID\":2,\"TS\":1704067200123456000,\"TXT\":\"after\",\"NS\":\"2.5\"}"));
        read(row(100, 4, newColumns, "{\"ID\":3,\"TS\":1704067200654321000,\"TXT\":\"later\",\"NS\":\"3.33\"}"));

        // one reload, before the first change of the new type; NS keeps the snapshot's scale
        assertThat(schemaChanges).hasSize(1);
        assertThat(schema.tableFor(TABLE_ID).columnWithName("NS").scale()).contains(2);
        assertThat(schemaChangeOffsets.get(0)).containsEntry("scn", "100").containsEntry("scn_idx", 2L);
        assertThat(schema.tableFor(TABLE_ID).columnWithName("TS").typeName()).isEqualTo("TIMESTAMP");
        assertThat(schema.schemaFor(TABLE_ID).valueSchema().field("TS").schema().name()).isEqualTo(MicroTimestamp.SCHEMA_NAME);
        assertThat(emittedValues()).containsExactly(3, 1704067200654321000L, "later", "3.33");
    }

    @Test
    void shouldReloadModelWhenVarcharIsWidened() throws Exception {
        snapshotDdl = ddl("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"TXT\" VARCHAR2(10), \"WIDE\" VARCHAR2(10 CHAR)");
        final String newDdl = ddl("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"TXT\" VARCHAR2(50), \"WIDE\" VARCHAR2(10 CHAR)");
        start(newDdl);

        // WIDE has character semantics: 40 bytes in AL32UTF8 is its unchanged length
        read(begin(100, 1));
        read(row(100, 2, "[" + id() + "," + varchar("TXT", 10) + "," + varchar("WIDE", 40) + "]", "{\"ID\":1,\"TXT\":\"before\",\"WIDE\":\"w\"}"));
        verify(dispatcher, never()).dispatchSchemaChangeEvent(any(), any(), any(), any());

        final String newColumns = "[" + id() + "," + varchar("TXT", 50) + "," + varchar("WIDE", 40) + "]";
        read(row(100, 3, newColumns, "{\"ID\":2,\"TXT\":\"a value longer than ten\",\"WIDE\":\"w\"}"));
        read(row(100, 4, newColumns, "{\"ID\":3,\"TXT\":\"later\",\"WIDE\":\"w\"}"));

        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0).getDdl()).isEqualTo(newDdl);
        assertThat(schema.tableFor(TABLE_ID).columnWithName("TXT").length()).isEqualTo(50);
        assertThat(schema.tableFor(TABLE_ID).columnWithName("WIDE").length()).isEqualTo(10);
        assertThat(emittedValues()).containsExactly(3, "later", "w");
    }

    @Test
    void shouldReloadOnceForNumberWithAsterisk() throws Exception {
        // OracleDdlParser parses NUMBER(*) with scale 0; OpenLogReplicator sends no precision and no scale
        assertReloadedOnceForParserQuirk("\"N\" NUMBER(*)", "\"N\" NUMBER", number("N", -1, -1), "\"N\":\"1.5\"");
        assertThat(schema.tableFor(TABLE_ID).columnWithName("N").scale()).isEmpty();
    }

    @Test
    void shouldReloadOnceForCharacterVarying() throws Exception {
        // OracleDdlParser parses CHARACTER VARYING as CHAR; the database stores VARCHAR2
        assertReloadedOnceForParserQuirk("\"V\" CHARACTER VARYING(10)", "\"V\" VARCHAR2(10)", varchar("V", 10), "\"V\":\"x\"");
        assertThat(schema.tableFor(TABLE_ID).columnWithName("V").typeName()).isEqualTo("VARCHAR2");
    }

    @Test
    void shouldNotReloadForLongRaw() throws Exception {
        // OpenLogReplicator's default unknown-type HIDE never sends LONG RAW values; one that arrives fits the model too
        snapshotDdl = ddl("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"LR\" LONG RAW");
        start(snapshotDdl);
        read(begin(100, 1));
        final String columns = "[" + id() + "," + column("LR", "\"type\":\"long raw\"") + "]";
        read(row(100, 2, columns, "{\"ID\":1}"));
        read(row(100, 3, columns, "{\"ID\":2,\"LR\":\"00\"}"));

        verify(connection, never()).getTableMetadataDdl(TABLE_ID);
        verify(dispatcher, never()).dispatchSchemaChangeEvent(any(), any(), any(), any());
        verify(dispatcher, times(2)).dispatchDataChangeEvent(any(), eq(TABLE_ID), any());
    }

    @Test
    void shouldReloadModelOfTableWithLongRaw() throws Exception {
        snapshotDdl = ddl("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"TXT\" VARCHAR2(10)");
        final String newDdl = ddl("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"TXT\" VARCHAR2(50), \"LR\" LONG RAW");
        start(newDdl);

        read(begin(100, 1));
        final String newColumns = "[" + id() + "," + varchar("TXT", 50) + "," + column("LR", "\"type\":\"long raw\"") + "]";
        read(row(100, 2, newColumns, "{\"ID\":1,\"TXT\":\"a value longer than ten\"}"));
        read(row(100, 3, newColumns, "{\"ID\":2,\"TXT\":\"later\"}"));

        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0).getDdl()).isEqualTo(newDdl);
        verify(connection).getTableMetadataDdl(TABLE_ID);
        final Table model = schema.tableFor(TABLE_ID);
        assertThat(model.columnWithName("TXT").length()).isEqualTo(50);
        assertThat(model.columnWithName("LR").typeName()).isEqualTo("LONG RAW");
        assertThat(model.columnWithName("LR").jdbcType()).isEqualTo(Types.LONGVARBINARY);
        assertThat(emittedValues()).containsExactly(2, "later", null);
    }

    /** A column parsed differently from how OpenLogReplicator describes it is reloaded once, then fits every change. */
    private void assertReloadedOnceForParserQuirk(String snapshotColumn, String databaseColumn, String metadata, String value) throws Exception {
        snapshotDdl = ddl("\"ID\" NUMBER(10,0) NOT NULL ENABLE, " + snapshotColumn);
        final String databaseDdl = ddl("\"ID\" NUMBER(10,0) NOT NULL ENABLE, " + databaseColumn);
        start(databaseDdl);
        final String columns = "[" + id() + "," + metadata + "]";
        read(begin(100, 1));
        for (int i = 2; i <= 5; i++) {
            read(row(100, i, columns, "{\"ID\":" + i + "," + value + "}"));
        }

        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0).getDdl()).isEqualTo(databaseDdl);
        verify(connection).getTableMetadataDdl(TABLE_ID);
        verify(dispatcher, times(4)).dispatchDataChangeEvent(any(), eq(TABLE_ID), any());
    }

    @Test
    void shouldAddColumnDroppedAgainBeforeCatchUp() throws Exception {
        // EXTRA came with the redefinition and was dropped before the connector reached the change
        start(ddl("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"TXT\" VARCHAR2(80), \"QTY\" NUMBER(18,4)"));
        read(begin(200, 1));
        read(row(200, 2, NEW_COLUMNS, NEW_ROW));

        final Table model = schema.tableFor(TABLE_ID);
        assertThat(model.retrieveColumnNames()).containsExactly("ID", "TXT", "QTY", "EXTRA");
        assertThat(model.columnWithName("EXTRA").typeName()).isEqualTo("VARCHAR2");
        assertThat(model.columnWithName("EXTRA").length()).isEqualTo(20);
        assertThat(model.primaryKeyColumnNames()).containsExactly("ID");
        assertThat(emittedValues()).containsExactly(1, "after", "7.1255", "set");
        assertRecordedModelParsesFromItsDdl();
    }

    @Test
    void shouldUseChangePrecisionModifiedAgainBeforeCatchUp() throws Exception {
        start(ddl("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"TXT\" VARCHAR2(80), \"QTY\" NUMBER(20,6), \"EXTRA\" VARCHAR2(20)"));
        read(begin(200, 1));
        read(row(200, 2, NEW_COLUMNS, NEW_ROW));

        assertThat(schema.tableFor(TABLE_ID).columnWithName("QTY").length()).isEqualTo(18);
        assertThat(schema.tableFor(TABLE_ID).columnWithName("QTY").scale()).contains(4);
        assertThat(emittedValues()).containsExactly(1, "after", "7.1255", "set");
        assertRecordedModelParsesFromItsDdl();

        // a change written after the MODIFY reloads the database's model as it is
        read(row(200, 3, "[" + id() + "," + varchar("TXT", 80) + "," + number("QTY", 20, 6) + "," + varchar("EXTRA", 20) + "]",
                "{\"ID\":2,\"TXT\":\"later\",\"QTY\":\"1.123456\",\"EXTRA\":\"x\"}"));
        assertThat(schemaChanges).hasSize(2);
        assertThat(schemaChanges.get(1).getDdl()).startsWith("CREATE TABLE \"DEBEZIUM\".\"CAPTURED\" \n");
        assertThat(schema.tableFor(TABLE_ID).columnWithName("QTY").scale()).contains(6);
    }

    @Test
    void shouldLeaveOutColumnAddedAgainBeforeCatchUp() throws Exception {
        start(ddl("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"TXT\" VARCHAR2(80), \"QTY\" NUMBER(18,4), \"EXTRA\" VARCHAR2(20), \"LATER\" DATE"));
        read(begin(200, 1));
        read(row(200, 2, NEW_COLUMNS, NEW_ROW));

        assertThat(schema.tableFor(TABLE_ID).retrieveColumnNames()).containsExactly("ID", "TXT", "QTY", "EXTRA");
        assertThat(emittedValues()).containsExactly(1, "after", "7.1255", "set");
        assertRecordedModelParsesFromItsDdl();
    }

    @Test
    void shouldBuildModelOfTableDroppedBeforeCatchUp() throws Exception {
        start(NEW_DDL);
        when(connection.getTableMetadataDdl(TABLE_ID)).thenThrow(new NonRelationalTableException("Table " + TABLE_ID + " was not found in ALL_ALL_TABLES."));
        read(begin(200, 1));
        read(row(200, 2, NEW_COLUMNS, NEW_ROW));

        // the last known model's columns where they still fit, the change's metadata otherwise
        final Table model = schema.tableFor(TABLE_ID);
        assertThat(model.retrieveColumnNames()).containsExactly("ID", "QTY", "TXT", "EXTRA");
        assertThat(model.columnWithName("QTY").length()).isEqualTo(18);
        assertThat(model.columnWithName("TXT").length()).isEqualTo(40);
        assertThat(model.primaryKeyColumnNames()).containsExactly("ID");
        assertThat(schemaChangeOffsets.get(0)).containsEntry("scn", "200").containsEntry("scn_idx", 1L);
        assertThat(emittedValues()).containsExactly(1, "7.1255", "after", "set");
        assertRecordedModelParsesFromItsDdl();
        verify(connection).resetSessionToCdb();
    }

    @Test
    void shouldBuildModelOfTableDroppedWhileItsDdlIsRead() throws Exception {
        start(NEW_DDL);
        when(connection.getTableMetadataDdl(TABLE_ID)).thenThrow(new SQLException("ORA-31603: object \"CAPTURED\" of type TABLE not found", "99999", 31603));
        read(begin(200, 1));
        read(row(200, 2, NEW_COLUMNS, NEW_ROW));

        assertThat(schema.tableFor(TABLE_ID).retrieveColumnNames()).containsExactly("ID", "QTY", "TXT", "EXTRA");
        assertThat(emittedValues()).containsExactly(1, "7.1255", "after", "set");
    }

    @Test
    void shouldFailBeforeRecordingModelWithoutKey() throws Exception {
        start(NEW_DDL);
        when(connection.getTableMetadataDdl(TABLE_ID)).thenThrow(new NonRelationalTableException("Table " + TABLE_ID + " was not found in ALL_ALL_TABLES."));
        read(begin(200, 1));

        assertThatThrownBy(() -> read(row(200, 2, RENAMED_KEY_COLUMNS, RENAMED_KEY_ROW)))
                .isInstanceOf(DebeziumException.class)
                .isNotInstanceOf(RetriableException.class)
                .hasMessageContaining("The model of table " + TABLE_ID + " as of the change at SCN 200 index 2 has no message key")
                .hasMessageContaining("[ID]")
                .hasMessageContaining("message.key.columns");
        verify(dispatcher, never()).dispatchSchemaChangeEvent(any(), any(), any(), any());
        verify(dispatcher, never()).dispatchDataChangeEvent(any(), any(), any());
        assertThat(schema.tableFor(TABLE_ID)).isEqualTo(parse(OLD_DDL));
    }

    @Test
    void shouldRecordModelWithoutPrimaryKeyWhenMessageKeyColumnsExist() throws Exception {
        start(NEW_DDL, builder -> builder.with(OracleConnectorConfig.PDB_NAME, "PDB1").with(OracleConnectorConfig.MSG_KEY_COLUMNS, "S1.DEBEZIUM.CAPTURED:ID2"));
        when(connection.getTableMetadataDdl(TABLE_ID)).thenThrow(new NonRelationalTableException("Table " + TABLE_ID + " was not found in ALL_ALL_TABLES."));
        read(begin(200, 1));
        read(row(200, 2, RENAMED_KEY_COLUMNS, RENAMED_KEY_ROW));

        final Table model = schema.tableFor(TABLE_ID);
        assertThat(model.primaryKeyColumnNames()).isEmpty();
        assertThat(connectorConfig.getKeyMapper().getKeyKolumns(model)).extracting(Column::name).containsExactly("ID2");
        assertThat(schema.schemaFor(TABLE_ID).keySchema().fields()).extracting(org.apache.kafka.connect.data.Field::name).containsExactly("ID2");
        assertThat(emittedValues()).containsExactlyInAnyOrder("7.1255", "after", 1, "set");
    }

    @Test
    void shouldFailWhenSessionResetFailsAfterTableIsGone() throws Exception {
        start(NEW_DDL);
        when(connection.getTableMetadataDdl(TABLE_ID)).thenThrow(new NonRelationalTableException("Table " + TABLE_ID + " was not found in ALL_ALL_TABLES."));
        doThrow(new RuntimeException(new SQLException("ORA-03114: not connected to ORACLE"))).when(connection).resetSessionToCdb();
        read(begin(200, 1));

        assertThatThrownBy(() -> read(row(200, 2, NEW_COLUMNS, NEW_ROW)))
                .isInstanceOf(NonRelationalTableException.class)
                .satisfies(e -> assertThat(e.getSuppressed()).singleElement()
                        .satisfies(suppressed -> assertThat(suppressed).hasRootCauseMessage("ORA-03114: not connected to ORACLE")));
        verify(dispatcher, never()).dispatchSchemaChangeEvent(any(), any(), any(), any());
        verify(dispatcher, never()).dispatchDataChangeEvent(any(), any(), any());
    }

    @Test
    void shouldFailWhenColumnTypeCannotBeDerived() throws Exception {
        start(NEW_DDL);
        when(connection.getTableMetadataDdl(TABLE_ID)).thenThrow(new NonRelationalTableException("Table " + TABLE_ID + " was not found in ALL_ALL_TABLES."));
        read(begin(200, 1));

        assertThatThrownBy(() -> read(row(200, 2, "[" + id() + "," + number("QTY", 15, 3) + "," + varchar("TXT", 40) + ","
                + "{\"name\":\"TS\",\"type\":\"timestamp\",\"length\":11,\"nullable\":true}]", "{\"ID\":1,\"QTY\":\"1.5\",\"TXT\":\"x\",\"TS\":\"2024-01-01\"}")))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("The model of table " + TABLE_ID + " cannot be built for the change at SCN 200 index 2")
                .hasMessageContaining("column TS of table " + TABLE_ID + " is timestamp length 11 in the change");
        verify(dispatcher, never()).dispatchSchemaChangeEvent(any(), any(), any(), any());
        verify(dispatcher, never()).dispatchDataChangeEvent(any(), any(), any());
    }

    @Test
    void shouldFailWhenNoModelFitsChange() throws Exception {
        // without column metadata nothing but the database describes the table
        start(OLD_DDL);
        read(begin(200, 1));

        assertThatThrownBy(() -> read("{" + header(200, 2) + "\"payload\":[{\"op\":\"c\",\"schema\":{\"owner\":\"DEBEZIUM\",\"table\":\"CAPTURED\"},"
                + "\"after\":" + NEW_ROW + "}]}"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("does not match the change at SCN 200 index 2 even after reloading it")
                .hasMessageContaining("column EXTRA is not in the model");
        verify(dispatcher, never()).dispatchDataChangeEvent(any(), any(), any());
    }

    @Test
    void shouldResetSessionWhenDdlCannotBeRead() throws Exception {
        start(NEW_DDL);
        when(connection.getTableMetadataDdl(TABLE_ID)).thenThrow(new SQLException("ORA-01031: insufficient privileges", "42000", 1031));
        read(begin(200, 1));

        assertThatThrownBy(() -> read(row(200, 2, NEW_COLUMNS, NEW_ROW)))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("ORA-01031");
        final InOrder session = inOrder(connection);
        session.verify(connection).setSessionToPdb("PDB1");
        session.verify(connection).resetSessionToCdb();
    }

    @Test
    void shouldKeepDdlFailureWhenSessionResetFailsToo() throws Exception {
        start(NEW_DDL);
        when(connection.getTableMetadataDdl(TABLE_ID)).thenThrow(new SQLException("ORA-03113: end-of-file on communication channel", "08000", 3113));
        doThrow(new RuntimeException(new SQLException("ORA-03114: not connected to ORACLE"))).when(connection).resetSessionToCdb();
        read(begin(200, 1));

        assertThatThrownBy(() -> read(row(200, 2, NEW_COLUMNS, NEW_ROW)))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("ORA-03113")
                .satisfies(e -> assertThat(e.getSuppressed()).singleElement()
                        .satisfies(suppressed -> assertThat(suppressed).hasRootCauseMessage("ORA-03114: not connected to ORACLE")));
    }

    @Test
    void shouldReadDdlInCurrentContainerWithoutPdb() throws Exception {
        start(NEW_DDL, builder -> builder.without(OracleConnectorConfig.PDB_NAME.name()));
        read(begin(200, 1));
        read(row(200, 2, NEW_COLUMNS, NEW_ROW));

        verify(connection).getTableMetadataDdl(TABLE_ID);
        verify(connection, never()).setSessionToPdb(anyString());
        verify(connection, never()).resetSessionToCdb();
        assertThat(emittedValues()).containsExactly(1, "after", "7.1255", "set");
    }

    @Test
    void shouldRecoverAndReplayAroundRecordedModel() throws Exception {
        // the table was dropped before the connector caught up, so only the history has the model of the change
        start(NEW_DDL);
        when(connection.getTableMetadataDdl(TABLE_ID)).thenThrow(new NonRelationalTableException("Table " + TABLE_ID + " was not found in ALL_ALL_TABLES."));
        read(begin(200, 1));
        read(row(200, 4, NEW_COLUMNS, NEW_ROW));
        final Table recorded = schema.tableFor(TABLE_ID);
        assertThat(schemaChangeOffsets.get(0)).containsEntry("scn", "200").containsEntry("scn_idx", 3L).containsEntry("txId", XID);

        // restart from an earlier transaction: the record is not applied, the earlier changes keep their model
        recover(offset(100, 2L, "0x0001.001.00000001"));
        assertThat(schema.tableFor(TABLE_ID)).isEqualTo(parse(OLD_DDL));

        // restart from the record's own offset: the record is applied and the change is not skipped
        recover(offset(200, 3L, XID));
        assertThat(schema.tableFor(TABLE_ID)).isEqualTo(recorded);

        clearInvocations(dispatcher, connection, connectionFactory);
        source = newSource(offset(200, 3L, XID));
        setField(source, "replayScn", Scn.valueOf(200));
        setField(source, "replayScnIndex", 3L);
        setField(source, "replayTransactionId", XID);
        read(begin(200, 1));
        read(row(200, 4, NEW_COLUMNS, NEW_ROW));

        verify(connectionFactory, never()).mainConnection();
        verify(dispatcher, never()).dispatchSchemaChangeEvent(any(), any(), any(), any());
        assertThat(emittedValues()).containsExactly(1, "7.1255", "after", "set");
    }

    private void recover(OracleOffsetContext offsetContext) throws InterruptedException {
        schema.getTables().clear();
        schema.recover(Offsets.of(partition, offsetContext));
    }

    /** With {@code schema.history.internal.prefer.ddl}, recovery parses the recorded DDL instead of the table change. */
    private void assertRecordedModelParsesFromItsDdl() {
        final SchemaChangeEvent event = schemaChanges.get(schemaChanges.size() - 1);
        assertThat(event.getDdl()).startsWith("CREATE TABLE \"DEBEZIUM\".\"CAPTURED\" (");
        final Table parsed = parse(event.getDdl());
        assertThat(parsed.retrieveColumnNames()).isEqualTo(event.getTables().iterator().next().retrieveColumnNames());
        assertThat(parsed).isEqualTo(schema.tableFor(TABLE_ID));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private OracleDatabaseSchema createSchema() {
        final TopicNamingStrategy topicNamingStrategy = SchemaTopicNamingStrategy.create(connectorConfig);
        final OracleValueConverters converters = connectorConfig.getAdapter().getValueConverter(connectorConfig, connection);
        final OracleDatabaseSchema databaseSchema = new OracleDatabaseSchema(connectorConfig, converters, new OracleDefaultValueConverter(converters, connection),
                connectorConfig.schemaNameAdjuster(), topicNamingStrategy, StreamingAdapter.TableNameCaseSensitivity.INSENSITIVE,
                false, new CustomConverterRegistry(List.of()), new OracleTaskContext(connectorConfig.getConfig(), connectorConfig));
        databaseSchema.initializeStorage();
        return databaseSchema;
    }

    private OracleOffsetContext offset(long scn, Long index, String transactionId) {
        return OracleOffsetContext.create()
                .logicalName(connectorConfig)
                .scn(Scn.valueOf(scn))
                .scnIndex(index)
                .transactionId(transactionId)
                .commitScn(CommitScn.empty())
                .snapshotPendingTransactions(Collections.emptyMap())
                .transactionContext(new TransactionContext())
                .incrementalSnapshotContext(new SignalBasedIncrementalSnapshotContext<>())
                .build();
    }

    private Object[] emittedValues() throws Exception {
        final ArgumentCaptor<ChangeRecordEmitter> emitter = ArgumentCaptor.forClass(ChangeRecordEmitter.class);
        verify(dispatcher, atLeastOnce()).dispatchDataChangeEvent(any(), eq(TABLE_ID), emitter.capture());
        final Field field = BaseChangeRecordEmitter.class.getDeclaredField("newColumnValues");
        field.setAccessible(true);
        return (Object[]) field.get(emitter.getAllValues().get(emitter.getAllValues().size() - 1));
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        final Field field = OpenLogReplicatorStreamingChangeEventSource.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Table parse(String ddl) {
        final OracleDdlParser parser = new OracleDdlParser();
        parser.setCurrentDatabase(TABLE_ID.catalog());
        parser.setCurrentSchema(TABLE_ID.schema());
        final Tables tables = new Tables();
        parser.parse(ddl, tables);
        return tables.forTable(TABLE_ID);
    }

    /** A table as {@code DBMS_METADATA.GET_DDL} renders it, with a primary key on {@code ID}. */
    private static String ddl(String columns) {
        return "CREATE TABLE \"DEBEZIUM\".\"CAPTURED\" \n   (\t" + columns.replace(", \"", ", \n\t\"")
                + ", \n\t PRIMARY KEY (\"ID\")\n  USING INDEX  ENABLE\n   ) ;";
    }

    private static String id() {
        return "{\"name\":\"ID\",\"type\":\"number\",\"precision\":10,\"scale\":0,\"nullable\":false}";
    }

    private static String number(String name, int precision, int scale) {
        return "{\"name\":\"" + name + "\",\"type\":\"number\",\"precision\":" + precision + ",\"scale\":" + scale + ",\"nullable\":true}";
    }

    private static String column(String name, String metadata) {
        return "{\"name\":\"" + name + "\"," + metadata + ",\"nullable\":true}";
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
