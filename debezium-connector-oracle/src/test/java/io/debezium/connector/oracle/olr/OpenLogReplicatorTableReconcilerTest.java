/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Types;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.olr.OpenLogReplicatorTableReconciler.Result;
import io.debezium.connector.oracle.olr.OpenLogReplicatorTableReconciler.UnmappableColumnException;
import io.debezium.connector.oracle.olr.client.StreamingEvent;
import io.debezium.connector.oracle.olr.client.payloads.AbstractMutationEvent;
import io.debezium.connector.oracle.olr.client.payloads.SchemaColumn;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;

import oracle.jdbc.OracleTypes;
import oracle.sql.CharacterSet;

/**
 * Maps OpenLogReplicator's column metadata to the columns {@link OracleDdlParser} builds from {@code DBMS_METADATA}
 * DDL, and adjusts a model to a change's metadata.
 */
public class OpenLogReplicatorTableReconcilerTest {

    private static final TableId TABLE_ID = new TableId("ORCLPDB1", "DEBEZIUM", "T");

    /** The key column as OpenLogReplicator describes {@code "ID" NUMBER(10,0) NOT NULL ENABLE}. */
    private static final String ID = col("ID", "\"type\":\"number\",\"precision\":10,\"scale\":0,\"nullable\":false");

    private final ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private OracleValueConverters converters;

    @BeforeEach
    void setUp() {
        final OracleConnection connection = Mockito.mock(OracleConnection.class);
        Mockito.when(connection.getNationalCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.AL16UTF16_CHARSET));
        Mockito.when(connection.getDatabaseCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.AL32UTF8_CHARSET));
        converters = new OracleValueConverters(new OracleConnectorConfig(TestHelper.defaultConfig().build()), connection);
    }

    /** The DDL type, OpenLogReplicator's description of it for an AL32UTF8 database, and whether the mapping is exact. */
    static Stream<Arguments> exactTypes() {
        return Stream.of(
                Arguments.of("NUMBER", number(-1, -1)),
                Arguments.of("NUMBER(10,0)", number(10, 0)),
                Arguments.of("NUMBER(15,3)", number(15, 3)),
                Arguments.of("NUMBER(10,-2)", number(10, -2)),
                Arguments.of("NUMBER(38,0)", number(38, 0)),
                Arguments.of("NUMBER(*,0)", number(-1, 0)),
                Arguments.of("FLOAT(126)", number(126, -1)),
                Arguments.of("FLOAT(63)", number(63, -1)),
                Arguments.of("FLOAT(1)", number(1, -1)),
                Arguments.of("BINARY_FLOAT", plain("binary_float")),
                Arguments.of("BINARY_DOUBLE", plain("binary_double")),
                Arguments.of("VARCHAR2(40)", sized("varchar2", 40)),
                Arguments.of("VARCHAR2(40 BYTE)", sized("varchar2", 40)),
                Arguments.of("VARCHAR2(4000)", sized("varchar2", 4000)),
                Arguments.of("CHAR(5)", sized("char", 5)),
                Arguments.of("CHAR(5 BYTE)", sized("char", 5)),
                Arguments.of("DATE", plain("date")),
                Arguments.of("TIMESTAMP (0)", sized("timestamp", 7)),
                Arguments.of("RAW(16)", sized("raw", 16)),
                Arguments.of("CLOB", plain("clob")),
                Arguments.of("BLOB", plain("blob")),
                Arguments.of("VARCHAR2(10) NOT NULL ENABLE", "\"type\":\"varchar2\",\"length\":10,\"nullable\":false"),
                Arguments.of("NUMBER(5,2) NOT NULL ENABLE", "\"type\":\"number\",\"precision\":5,\"scale\":2,\"nullable\":false"));
    }

    /** Types the metadata does not tell apart from another; mapped to a column with the same Kafka Connect schema. */
    static Stream<Arguments> sameSchemaTypes() {
        return Stream.of(
                Arguments.of("NVARCHAR2(20)", sized("varchar2", 40)),
                Arguments.of("VARCHAR2(40 CHAR)", sized("varchar2", 160)),
                Arguments.of("NCHAR(5)", sized("char", 10)),
                Arguments.of("CHAR(5 CHAR)", sized("char", 20)),
                Arguments.of("NCLOB", plain("clob")));
    }

    /** Types whose metadata does not determine the column, e.g. a timestamp's fractional precision. */
    static Stream<Arguments> unmappableTypes() {
        return Stream.of(
                Arguments.of("TIMESTAMP (3)", sized("timestamp", 11)),
                Arguments.of("TIMESTAMP (6)", sized("timestamp", 11)),
                Arguments.of("TIMESTAMP (9)", sized("timestamp", 11)),
                Arguments.of("TIMESTAMP (6) WITH TIME ZONE", sized("timestamp with time zone", 13)),
                Arguments.of("TIMESTAMP (0) WITH TIME ZONE", sized("timestamp with time zone", 13)),
                Arguments.of("TIMESTAMP (6) WITH LOCAL TIME ZONE", sized("timestamp with local time zone", 11)),
                Arguments.of("INTERVAL YEAR (2) TO MONTH", sized("interval year to month", 5)),
                Arguments.of("INTERVAL DAY (2) TO SECOND (6)", sized("interval day to second", 11)));
    }

    @ParameterizedTest
    @MethodSource("exactTypes")
    void shouldMapMetadataToParsedColumn(String ddlType, String metadata) throws Exception {
        final Column parsed = parseColumn(ddlType);
        final SchemaColumn schemaColumn = schemaColumn(metadata);

        assertThat(describe(OpenLogReplicatorTableReconciler.toColumn(table(), schemaColumn))).isEqualTo(describe(parsed));
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(parsed, schemaColumn)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("sameSchemaTypes")
    void shouldMapIndistinguishableTypesToSameConnectSchema(String ddlType, String metadata) throws Exception {
        final Column parsed = parseColumn(ddlType);
        final SchemaColumn schemaColumn = schemaColumn(metadata);
        final Column mapped = OpenLogReplicatorTableReconciler.toColumn(table(), schemaColumn);

        assertThat(converters.schemaBuilder(mapped).build()).isEqualTo(converters.schemaBuilder(parsed).build());
        // a known model keeps the exact type
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(parsed, schemaColumn)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("unmappableTypes")
    void shouldRefuseToMapUndeterminedType(String ddlType, String metadata) throws Exception {
        final SchemaColumn schemaColumn = schemaColumn(metadata);

        assertThatThrownBy(() -> OpenLogReplicatorTableReconciler.toColumn(table(), schemaColumn))
                .isInstanceOf(UnmappableColumnException.class)
                .hasMessageContaining("column C of table " + TABLE_ID);
        // a known model keeps the exact type
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(parseColumn(ddlType), schemaColumn)).isTrue();
    }

    @Test
    void shouldMapNumberWithAsteriskAndScaleToItsScale() throws Exception {
        // OracleDdlParser parses NUMBER(*,2) with scale 0; the metadata's scale is the column's
        final Column mapped = OpenLogReplicatorTableReconciler.toColumn(table(), schemaColumn(number(-1, 2)));
        assertThat(mapped.length()).isEqualTo(38);
        assertThat(mapped.scale()).contains(2);
        // a parsed NUMBER(*,2) has scale 0 and converts 1.25 to null: replaced by a reload
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(parseColumn("NUMBER(*,2)"), schemaColumn(number(-1, 2)))).isFalse();
    }

    @Test
    void shouldKeepKnownNumberWithScaleMinusOneButMapNewOneToFloat() throws Exception {
        // NUMBER(p,-1) and FLOAT(p) are both sent as precision p, scale -1
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(parseColumn("NUMBER(10,-1)"), schemaColumn(number(10, -1)))).isTrue();
        assertThat(describe(OpenLogReplicatorTableReconciler.toColumn(table(), schemaColumn(number(10, -1))))).isEqualTo(describe(parseColumn("FLOAT(10)")));
    }

    @Test
    void shouldKeepXmlTypeReportedAsLob() throws Exception {
        final Column parsed = parseColumn("XMLTYPE");
        final Column fromJdbc = Column.editor().name("C").jdbcType(OracleTypes.SQLXML).type("SYS.XMLTYPE").create();
        for (Column column : new Column[]{ parsed, fromJdbc }) {
            assertThat(OpenLogReplicatorTableReconciler.isCompatible(column, schemaColumn(plain("clob")))).isTrue();
            assertThat(OpenLogReplicatorTableReconciler.isCompatible(column, schemaColumn(plain("blob")))).isTrue();
        }

        final Table base = parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"X\" XMLTYPE");
        assertThat(reconcile(base, null, "[" + ID + "," + col("X", plain("clob")) + "]", "{\"ID\":1}", null).table()).isSameAs(base);

        // the database's X changed later; the last known XMLTYPE still fits the change
        final Result result = reconcile(parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"X\" VARCHAR2(10)"), base,
                "[" + ID + "," + col("X", plain("blob")) + "]", "{\"ID\":1}", null);
        assertThat(describe(result.table().columnWithName("X"))).isEqualTo(describe(parsed));
    }

    @Test
    void shouldAcceptModelsReadOverJdbc() throws Exception {
        // a snapshot model has the JDBC driver's type names and a NUMBER without precision has length 0
        final Column number = Column.editor().name("C").jdbcType(OracleTypes.NUMBER).type("NUMBER").length(0).create();
        final Column timestamp = Column.editor().name("C").jdbcType(OracleTypes.TIMESTAMP).type("TIMESTAMP(6)").length(6).create();
        final Column zoned = Column.editor().name("C").jdbcType(OracleTypes.TIMESTAMPTZ).type("TIMESTAMP(6) WITH TIME ZONE").length(6).create();
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(number, schemaColumn(number(-1, -1)))).isTrue();
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(timestamp, schemaColumn(sized("timestamp", 11)))).isTrue();
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(zoned, schemaColumn(sized("timestamp with time zone", 13)))).isTrue();
        // NUMBER(*,2) over JDBC: no precision, the dictionary's scale
        final Column numberWithScale = Column.editor().name("C").jdbcType(OracleTypes.NUMBER).type("NUMBER").length(0).scale(2).create();
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(numberWithScale, schemaColumn(number(-1, 2)))).isTrue();
        // OracleDdlParser cannot parse LONG RAW
        final Column longRaw = Column.editor().name("C").jdbcType(OracleTypes.LONGVARBINARY).type("LONG RAW").create();
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(longRaw, schemaColumn(plain("long raw")))).isTrue();
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(longRaw, schemaColumn(plain("blob")))).isFalse();
    }

    @Test
    void shouldDetectTypeChanges() throws Exception {
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(parseColumn("NUMBER(15,3)"), schemaColumn(number(18, 4)))).isFalse();
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(parseColumn("NUMBER(10,2)"), schemaColumn(number(-1, -1)))).isFalse();
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(parseColumn("NUMBER"), schemaColumn(number(10, 0)))).isFalse();
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(parseColumn("NUMBER(10,0)"), schemaColumn(sized("varchar2", 20)))).isFalse();
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(parseColumn("VARCHAR2(20)"), schemaColumn(number(10, 0)))).isFalse();
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(parseColumn("FLOAT(126)"), schemaColumn(number(63, -1)))).isFalse();
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(parseColumn("FLOAT(126)"), schemaColumn(number(126, 0)))).isFalse();
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(parseColumn("TIMESTAMP (0)"), schemaColumn(sized("timestamp", 11)))).isFalse();
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(parseColumn("TIMESTAMP (6)"), schemaColumn(sized("timestamp", 7)))).isFalse();
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(parseColumn("DATE"), schemaColumn(sized("timestamp", 11)))).isFalse();
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(parseColumn("RAW(16)"), schemaColumn(sized("raw", 32)))).isFalse();
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(parseColumn("CLOB"), schemaColumn(plain("blob")))).isFalse();
    }

    /** A model type and OpenLogReplicator's description of a changed column: the model must be reloaded. */
    static Stream<Arguments> changedTypes() {
        return Stream.of(
                Arguments.of("DATE", sized("timestamp", 11)),
                Arguments.of("DATE", sized("timestamp", 7)),
                Arguments.of("TIMESTAMP (6)", plain("date")),
                Arguments.of("TIMESTAMP (0)", sized("timestamp", 11)),
                Arguments.of("TIMESTAMP (6)", sized("timestamp", 7)),
                Arguments.of("TIMESTAMP (6)", sized("timestamp with time zone", 13)),
                Arguments.of("TIMESTAMP (6) WITH TIME ZONE", sized("timestamp with local time zone", 11)),
                Arguments.of("TIMESTAMP (6) WITH LOCAL TIME ZONE", sized("timestamp", 11)),
                Arguments.of("VARCHAR2(10)", sized("varchar2", 50)),
                Arguments.of("VARCHAR2(50)", sized("varchar2", 10)),
                Arguments.of("VARCHAR2(10 CHAR)", sized("varchar2", 200)),
                Arguments.of("VARCHAR2(4000)", sized("varchar2", 32767)),
                Arguments.of("NVARCHAR2(10)", sized("varchar2", 5)),
                Arguments.of("CHAR(5)", sized("char", 7)),
                Arguments.of("NCHAR(5)", sized("char", 22)),
                Arguments.of("VARCHAR2(20)", sized("char", 20)),
                Arguments.of("CHAR(20)", sized("varchar2", 20)),
                Arguments.of("VARCHAR2(20)", number(10, 0)),
                Arguments.of("VARCHAR2(4000)", plain("clob")),
                Arguments.of("CLOB", sized("varchar2", 4000)),
                Arguments.of("CLOB", plain("blob")),
                Arguments.of("BLOB", plain("clob")),
                Arguments.of("BLOB", sized("raw", 2000)),
                Arguments.of("RAW(16)", sized("raw", 32)),
                Arguments.of("RAW(16)", plain("blob")),
                Arguments.of("XMLTYPE", sized("varchar2", 4000)),
                Arguments.of("NUMBER(10,0)", sized("varchar2", 20)),
                Arguments.of("NUMBER(15,3)", number(18, 4)),
                Arguments.of("NUMBER(10,2)", number(-1, -1)),
                Arguments.of("NUMBER", number(10, 0)),
                Arguments.of("NUMBER", number(-1, 0)),
                Arguments.of("NUMBER(*,0)", number(-1, -1)),
                Arguments.of("NUMBER(*,2)", number(10, 2)),
                // OracleDdlParser parses NUMBER(*,2) with scale 0
                Arguments.of("NUMBER(*,2)", number(-1, 2)),
                Arguments.of("NUMBER(10,0)", plain("date")),
                Arguments.of("NUMBER(10,0)", plain("binary_double")),
                Arguments.of("FLOAT(126)", number(63, -1)),
                Arguments.of("FLOAT(126)", number(126, 0)),
                Arguments.of("BINARY_FLOAT", plain("binary_double")),
                Arguments.of("BINARY_DOUBLE", number(-1, -1)),
                Arguments.of("INTERVAL YEAR (2) TO MONTH", sized("interval day to second", 11)),
                Arguments.of("NUMBER(10,0)", plain("unknown")),
                Arguments.of("DATE", plain("unknown")),
                Arguments.of("VARCHAR2(10)", plain("unknown")));
    }

    /** A model type and OpenLogReplicator's description of the same, unchanged column: no reload. */
    static Stream<Arguments> unchangedTypes() {
        return Stream.concat(Stream.concat(exactTypes(), sameSchemaTypes()), Stream.concat(unmappableTypes(), Stream.of(
                Arguments.of("INTEGER", number(-1, 0)),
                Arguments.of("NUMBER(10)", number(10, 0)),
                Arguments.of("NUMBER(10,-1)", number(10, -1)),
                Arguments.of("FLOAT", number(126, -1)),
                Arguments.of("REAL", number(63, -1)),
                // the metadata has no fractional precision, only whether there is a fraction
                Arguments.of("TIMESTAMP (3)", sized("timestamp", 11)),
                Arguments.of("TIMESTAMP (9) WITH LOCAL TIME ZONE", sized("timestamp with local time zone", 11)),
                Arguments.of("TIMESTAMP (0) WITH TIME ZONE", sized("timestamp with time zone", 13)),
                // character lengths in bytes: AL32UTF8 (4), UTF8 (3), AL16UTF16 (2), capped at the type's byte limit
                Arguments.of("VARCHAR2(10 CHAR)", sized("varchar2", 30)),
                Arguments.of("VARCHAR2(1500 CHAR)", sized("varchar2", 4000)),
                Arguments.of("VARCHAR2(4000 CHAR)", sized("varchar2", 4000)),
                Arguments.of("VARCHAR2(10000 CHAR)", sized("varchar2", 32767)),
                Arguments.of("VARCHAR2(32767)", sized("varchar2", 32767)),
                Arguments.of("CHAR(600 CHAR)", sized("char", 2000)),
                Arguments.of("NVARCHAR2(10)", sized("varchar2", 30)),
                Arguments.of("NVARCHAR2(2000)", sized("varchar2", 4000)),
                Arguments.of("NCHAR(1000)", sized("char", 2000)),
                Arguments.of("XMLTYPE", plain("clob")),
                Arguments.of("XMLTYPE", plain("blob")),
                Arguments.of("XMLTYPE", plain("unknown")),
                Arguments.of("NCLOB", plain("clob")),
                Arguments.of("LONG", plain("long")),
                Arguments.of("UROWID", sized("urowid", 4000)),
                Arguments.of("INTERVAL YEAR (4) TO MONTH", sized("interval year to month", 5)),
                Arguments.of("SDO_GEOMETRY", plain("unknown")),
                Arguments.of("JSON", plain("unknown")),
                Arguments.of("BOOLEAN", plain("unknown")))));
    }

    @ParameterizedTest
    @MethodSource("changedTypes")
    void shouldDetectChangedType(String ddlType, String metadata) throws Exception {
        final Table model = parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"C\" " + ddlType);
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(model.columnWithName("C"), schemaColumn(metadata))).isFalse();
        assertThat(OpenLogReplicatorStreamingChangeEventSource.findModelMismatch(model, change("[" + ID + "," + col("C", metadata) + "]",
                "{\"ID\":1,\"C\":\"x\"}"), null)).startsWith("column C is ");
    }

    @ParameterizedTest
    @MethodSource("unchangedTypes")
    void shouldNotReloadUnchangedType(String ddlType, String metadata) throws Exception {
        final Table model = parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"C\" " + ddlType);
        assertThat(OpenLogReplicatorTableReconciler.isCompatible(model.columnWithName("C"), schemaColumn(metadata))).isTrue();
        assertThat(OpenLogReplicatorStreamingChangeEventSource.findModelMismatch(model, change("[" + ID + "," + col("C", metadata) + "]",
                "{\"ID\":1,\"C\":\"x\"}"), null)).isNull();
    }

    @Test
    void shouldNotCompareTypeOfColumnWithoutValue() throws Exception {
        final Table model = parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"C\" DATE");
        assertThat(OpenLogReplicatorStreamingChangeEventSource.findModelMismatch(model, change("[" + ID + "," + col("C", sized("timestamp", 11)) + "]",
                "{\"ID\":1}"), null)).isNull();
        assertThat(OpenLogReplicatorStreamingChangeEventSource.findModelMismatch(model, change("[" + ID + "," + col("C", sized("timestamp", 11)) + "]",
                "{\"ID\":1,\"C\":null}"), null)).isEqualTo("column C is timestamp length 11 in the change and DATE in the model");
    }

    /** Every column of an unchanged table, as DDL and as OpenLogReplicator describes it for an AL32UTF8 database. */
    private static final String[][] UNCHANGED_TABLE = {
            { "\"N\" NUMBER", number(-1, -1) },
            { "\"NP\" NUMBER(15,3)", number(15, 3) },
            { "\"NI\" INTEGER", number(-1, 0) },
            { "\"NN\" NUMBER(10,-2)", number(10, -2) },
            { "\"F\" FLOAT(126)", number(126, -1) },
            { "\"BF\" BINARY_FLOAT", plain("binary_float") },
            { "\"BD\" BINARY_DOUBLE", plain("binary_double") },
            { "\"V\" VARCHAR2(40 BYTE)", sized("varchar2", 40) },
            { "\"VC\" VARCHAR2(40 CHAR)", sized("varchar2", 160) },
            { "\"NV\" NVARCHAR2(20)", sized("varchar2", 40) },
            { "\"C\" CHAR(5 BYTE)", sized("char", 5) },
            { "\"CC\" CHAR(5 CHAR)", sized("char", 20) },
            { "\"NC\" NCHAR(5)", sized("char", 10) },
            { "\"D\" DATE", plain("date") },
            { "\"T0\" TIMESTAMP (0)", sized("timestamp", 7) },
            { "\"T6\" TIMESTAMP (6)", sized("timestamp", 11) },
            { "\"TZ\" TIMESTAMP (6) WITH TIME ZONE", sized("timestamp with time zone", 13) },
            { "\"LTZ\" TIMESTAMP (3) WITH LOCAL TIME ZONE", sized("timestamp with local time zone", 11) },
            { "\"YM\" INTERVAL YEAR (2) TO MONTH", sized("interval year to month", 5) },
            { "\"DS\" INTERVAL DAY (3) TO SECOND (6)", sized("interval day to second", 11) },
            { "\"R\" RAW(16)", sized("raw", 16) },
            { "\"CL\" CLOB", plain("clob") },
            { "\"NCL\" NCLOB", plain("clob") },
            { "\"BL\" BLOB", plain("blob") },
            { "\"X\" XMLTYPE", plain("clob") },
            { "\"XB\" XMLTYPE", plain("blob") } };

    @Test
    void shouldNeverReloadUnchangedTable() throws Exception {
        final StringBuilder ddl = new StringBuilder("\"ID\" NUMBER(10,0) NOT NULL ENABLE");
        final StringBuilder columns = new StringBuilder("[" + ID);
        final StringBuilder values = new StringBuilder("{\"ID\":1");
        for (String[] column : UNCHANGED_TABLE) {
            final String name = column[0].substring(1, column[0].indexOf('"', 1));
            ddl.append(", ").append(column[0]);
            columns.append(',').append(col(name, column[1]));
            values.append(",\"").append(name).append("\":\"x\"");
        }
        // hidden, guard and unused columns have no value
        columns.append(',').append(col("SYS_NC00031$", sized("raw", 16))).append(',').append(col("SYS_C00032_26103016:01:02$", plain("date"))).append(']');
        final AbstractMutationEvent change = change(columns.toString(), values.append('}').toString());

        final Table parsed = parseTable(ddl.toString());
        assertThat(OpenLogReplicatorStreamingChangeEventSource.findModelMismatch(parsed, change, null)).isNull();
        // the same table as a snapshot reads it over JDBC
        assertThat(OpenLogReplicatorStreamingChangeEventSource.findModelMismatch(asReadOverJdbc(parsed), change, null)).isNull();
        // and as the reconciler rebuilds it from the metadata
        assertThat(OpenLogReplicatorTableReconciler.reconcile(parsed, null, change, null).changes()).isEmpty();
    }

    /** The JDBC driver's type names and lengths: a NUMBER without precision has length 0, a timestamp has its precision as length. */
    private static Table asReadOverJdbc(Table parsed) {
        final TableEditor editor = parsed.edit();
        for (Column column : parsed.columns()) {
            final ColumnEditor jdbc = column.edit();
            switch (column.typeName()) {
                case "NUMBER":
                    if (column.length() == 38) {
                        jdbc.length(0);
                    }
                    break;
                case "TIMESTAMP":
                case "TIMESTAMP WITH TIME ZONE":
                case "TIMESTAMP WITH LOCAL TIME ZONE":
                    jdbc.type(column.typeName().replace("TIMESTAMP", "TIMESTAMP(" + column.length() + ")"));
                    break;
                case "INTERVAL YEAR TO MONTH":
                    jdbc.type("INTERVAL YEAR(" + column.length() + ") TO MONTH");
                    break;
                case "INTERVAL DAY TO SECOND":
                    jdbc.type("INTERVAL DAY(" + column.length() + ") TO SECOND(" + column.scale().orElse(6) + ")");
                    break;
                case "XMLTYPE":
                    jdbc.type("SYS.XMLTYPE");
                    break;
                default:
                    break;
            }
            editor.addColumn(jdbc.create());
        }
        return editor.create();
    }

    @Test
    void shouldKeepModelThatMatchesMetadata() throws Exception {
        final Table base = parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"QTY\" NUMBER(15,3), \"TXT\" VARCHAR2(40 CHAR)");
        final Result result = reconcile(base, null, "[" + ID + "," + col("QTY", number(15, 3)) + "," + col("TXT", sized("varchar2", 160))
                + "," + col("SYS_NC00004$", sized("raw", 16)) + "]", "{\"ID\":1,\"QTY\":\"1.5\",\"TXT\":\"x\"}", null);

        assertThat(result.changes()).isEmpty();
        assertThat(result.table()).isSameAs(base);
    }

    @Test
    void shouldDropColumnsMissingFromMetadataOnly() throws Exception {
        // NOTE has no value but is in the list (e.g. a LOB not sent); LATER is not in the list: added after the change
        final Table base = parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"NOTE\" CLOB, \"LATER\" VARCHAR2(10)");
        final Result result = reconcile(base, null, "[" + ID + "," + col("NOTE", plain("clob")) + "]", "{\"ID\":1}", null);

        assertThat(result.table().retrieveColumnNames()).containsExactly("ID", "NOTE");
        assertThat(result.table().primaryKeyColumnNames()).containsExactly("ID");
        assertThat(result.changes()).containsExactly("dropped LATER");
        assertThat(result.primaryKeyDropped()).isFalse();
    }

    @Test
    void shouldKeepGeneratedColumnMissingFromMetadata() throws Exception {
        final Table parsed = parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"QTY\" NUMBER(15,3)");
        final Table base = parsed.edit().addColumn(Column.editor().name("V").jdbcType(Types.NUMERIC).type("NUMBER").generated(true).create()).create();
        final Result result = reconcile(base, null, "[" + ID + "," + col("QTY", number(18, 4)) + "]", "{\"ID\":1,\"QTY\":\"1.5\"}", null);

        assertThat(result.table().retrieveColumnNames()).containsExactly("ID", "QTY", "V");
    }

    @Test
    void shouldReplaceChangedTypeAndKeepOtherColumns() throws Exception {
        final Table base = parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"QTY\" NUMBER(20,6) DEFAULT 0, \"TXT\" NVARCHAR2(20)");
        final Result result = reconcile(base, null, "[" + ID + "," + col("QTY", number(18, 4)) + "," + col("TXT", sized("varchar2", 40)) + "]",
                "{\"ID\":1,\"QTY\":\"1.5\",\"TXT\":\"x\"}", null);

        final Column qty = result.table().columnWithName("QTY");
        assertThat(qty.length()).isEqualTo(18);
        assertThat(qty.scale()).contains(4);
        assertThat(qty.position()).isEqualTo(2);
        assertThat(result.table().columnWithName("TXT").typeName()).isEqualTo("NVARCHAR2");
        assertThat(result.table().primaryKeyColumnNames()).containsExactly("ID");
        assertThat(result.changes()).containsExactly("changed type of QTY to number(18,4)");
    }

    @Test
    void shouldPreferLastKnownColumnForReplacedOrAddedColumn() throws Exception {
        final Table base = parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"TS\" DATE");
        final Table lastKnown = parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"TS\" TIMESTAMP (9), \"N\" NVARCHAR2(5)");
        final Result result = reconcile(base, lastKnown, "[" + ID + "," + col("TS", sized("timestamp", 11)) + "," + col("N", sized("varchar2", 10)) + "]",
                "{\"ID\":1,\"TS\":\"2024-01-01\",\"N\":\"x\"}", null);

        assertThat(result.table().columnWithName("TS").typeName()).isEqualTo("TIMESTAMP");
        assertThat(result.table().columnWithName("TS").length()).isEqualTo(9);
        assertThat(result.table().columnWithName("N").typeName()).isEqualTo("NVARCHAR2");
        assertThat(result.table().columnWithName("N").length()).isEqualTo(5);
    }

    @Test
    void shouldAddColumnWithValueFromMetadata() throws Exception {
        final Table base = parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE");
        final Result result = reconcile(base, null, "[" + ID + "," + col("EXTRA", "\"type\":\"varchar2\",\"length\":20,\"nullable\":false")
                + "," + col("HIDDEN", sized("varchar2", 20)) + "]", "{\"ID\":1,\"EXTRA\":\"x\"}", null);

        assertThat(result.table().retrieveColumnNames()).containsExactly("ID", "EXTRA");
        assertThat(describe(result.table().columnWithName("EXTRA"))).isEqualTo(describe(parseColumn("VARCHAR2(20) NOT NULL ENABLE")));
        assertThat(result.changes()).containsExactly("added EXTRA varchar2 length 20");
    }

    @Test
    void shouldNotAddExcludedColumn() throws Exception {
        final Table base = parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE");
        final ColumnNameFilter excludeExtra = (catalog, schema, table, column) -> !"EXTRA".equals(column);
        final Result result = reconcile(base, null, "[" + ID + "," + col("EXTRA", sized("varchar2", 20)) + "]",
                "{\"ID\":1,\"EXTRA\":\"x\"}", excludeExtra);

        assertThat(result.changes()).isEmpty();
    }

    @Test
    void shouldOnlyRelaxNullability() throws Exception {
        final Table base = parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"A\" VARCHAR2(5) NOT NULL ENABLE, \"B\" VARCHAR2(5)");
        final Result result = reconcile(base, null,
                "[" + ID + "," + col("A", sized("varchar2", 5)) + "," + col("B", "\"type\":\"varchar2\",\"length\":5,\"nullable\":false") + "]",
                "{\"ID\":1,\"A\":null,\"B\":\"b\"}", null);

        assertThat(result.table().columnWithName("ID").isOptional()).isFalse();
        assertThat(result.table().columnWithName("A").isOptional()).isTrue();
        assertThat(result.table().columnWithName("B").isOptional()).isTrue();
        assertThat(result.changes()).containsExactly("made nullable A");
    }

    @Test
    void shouldDropPrimaryKeyWhoseColumnIsGone() throws Exception {
        final Table base = parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"K\" NUMBER(10,0)");
        final Result result = reconcile(base, null, "[" + col("K", number(10, 0)) + "]", "{\"K\":1}", null);

        assertThat(result.table().primaryKeyColumnNames()).isEmpty();
        assertThat(result.primaryKeyDropped()).isTrue();
    }

    @Test
    void shouldFailForValueWithoutMetadata() throws Exception {
        final Table base = parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE");
        assertThatThrownBy(() -> reconcile(base, null, "[" + ID + "]", "{\"ID\":1,\"EXTRA\":\"x\"}", null))
                .isInstanceOf(UnmappableColumnException.class)
                .hasMessage("column EXTRA has a value but is not in the change's column list");
    }

    @Test
    void shouldFailForAddedColumnOfUndeterminedType() throws Exception {
        final Table base = parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE");
        assertThatThrownBy(() -> reconcile(base, null, "[" + ID + "," + col("TS", sized("timestamp", 11)) + "]",
                "{\"ID\":1,\"TS\":\"x\"}", null))
                .isInstanceOf(UnmappableColumnException.class)
                .hasMessageContaining("column TS of table " + TABLE_ID + " is timestamp length 11 in the change");
    }

    @Test
    void shouldKeepModelWithoutColumnMetadata() throws Exception {
        final Table base = parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE");
        final StreamingEvent event = mapper.readValue("{\"scn\":1,\"tm\":0,\"c_scn\":1,\"c_idx\":1,\"xid\":\"x\",\"db\":\"S1\",\"payload\":[{\"op\":\"c\","
                + "\"schema\":{\"owner\":\"DEBEZIUM\",\"table\":\"T\"},\"after\":{\"ID\":1,\"EXTRA\":\"x\"}}]}", StreamingEvent.class);
        final Result result = OpenLogReplicatorTableReconciler.reconcile(base, null, (AbstractMutationEvent) event.getPayload().get(0), null);
        assertThat(result.table()).isSameAs(base);
    }

    @Test
    void shouldRenderDdlThatParsesToSameModel() throws Exception {
        final Table table = parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"N\" NUMBER, \"I\" NUMBER(*,0), \"Q\" NUMBER(18,4) DEFAULT 0, "
                + "\"NEG\" NUMBER(10,-2), \"F\" FLOAT(126), \"BF\" BINARY_FLOAT, \"BD\" BINARY_DOUBLE, \"V\" VARCHAR2(40) NOT NULL ENABLE, "
                + "\"NV\" NVARCHAR2(20), \"C\" CHAR(5), \"NC\" NCHAR(5), \"D\" DATE DEFAULT SYSDATE, \"T0\" TIMESTAMP (0), \"T9\" TIMESTAMP (9), "
                + "\"TZ\" TIMESTAMP (6) WITH TIME ZONE, \"LTZ\" TIMESTAMP (3) WITH LOCAL TIME ZONE, \"R\" RAW(16), \"CL\" CLOB, \"NCL\" NCLOB, "
                + "\"BL\" BLOB, \"YM\" INTERVAL YEAR (2) TO MONTH, \"DS\" INTERVAL DAY (3) TO SECOND (6)");

        final String ddl = OpenLogReplicatorTableReconciler.toDdl(table);
        final Table parsed = parse(ddl);

        assertThat(parsed.primaryKeyColumnNames()).containsExactly("ID");
        assertThat(parsed.columns()).hasSameSizeAs(table.columns());
        for (Column column : table.columns()) {
            assertThat(describe(parsed.columnWithName(column.name()))).as(ddl).isEqualTo(describe(column));
            assertThat(parsed.columnWithName(column.name()).position()).isEqualTo(column.position());
        }
    }

    private Result reconcile(Table base, Table lastKnown, String columns, String after, ColumnNameFilter filter) throws Exception {
        final StreamingEvent event = mapper.readValue("{\"scn\":1,\"tm\":0,\"c_scn\":1,\"c_idx\":1,\"xid\":\"x\",\"db\":\"S1\",\"payload\":[{\"op\":\"c\","
                + "\"schema\":{\"owner\":\"DEBEZIUM\",\"table\":\"T\",\"obj\":7,\"columns\":" + columns + "},\"after\":" + after + "}]}", StreamingEvent.class);
        return OpenLogReplicatorTableReconciler.reconcile(base, lastKnown, (AbstractMutationEvent) event.getPayload().get(0), filter);
    }

    private AbstractMutationEvent change(String columns, String after) throws Exception {
        final StreamingEvent event = mapper.readValue("{\"scn\":1,\"tm\":0,\"c_scn\":1,\"c_idx\":1,\"xid\":\"x\",\"db\":\"S1\",\"payload\":[{\"op\":\"c\","
                + "\"schema\":{\"owner\":\"DEBEZIUM\",\"table\":\"T\",\"obj\":7,\"columns\":" + columns + "},\"after\":" + after + "}]}", StreamingEvent.class);
        return (AbstractMutationEvent) event.getPayload().get(0);
    }

    private SchemaColumn schemaColumn(String metadata) throws Exception {
        return mapper.readValue(col("C", metadata), SchemaColumn.class);
    }

    private static Table table() {
        return Table.editor().tableId(TABLE_ID).create();
    }

    private static Column parseColumn(String ddlType) {
        return parseTable("\"ID\" NUMBER(10,0) NOT NULL ENABLE, \"C\" " + ddlType).columnWithName("C");
    }

    /** A table as {@code DBMS_METADATA.GET_DDL} renders it, with a primary key on {@code ID}. */
    static Table parseTable(String columns) {
        return parse("CREATE TABLE \"DEBEZIUM\".\"T\" \n   (\t" + columns.replace(", \"", ", \n\t\"")
                + ", \n\t PRIMARY KEY (\"ID\")\n  USING INDEX  ENABLE\n   ) ;");
    }

    static Table parse(String ddl) {
        final OracleDdlParser parser = new OracleDdlParser();
        parser.setCurrentDatabase(TABLE_ID.catalog());
        parser.setCurrentSchema(TABLE_ID.schema());
        final Tables tables = new Tables();
        parser.parse(ddl, tables);
        return tables.forTable(TABLE_ID);
    }

    /** Every attribute of a column but its name and position. */
    static String describe(Column column) {
        return "jdbcType=" + column.jdbcType() + " nativeType=" + column.nativeType() + " typeName=" + column.typeName()
                + " typeExpression=" + column.typeExpression() + " length=" + column.length() + " scale=" + column.scale()
                + " optional=" + column.isOptional() + " charset=" + column.charsetName() + " generated=" + column.isGenerated()
                + " autoIncremented=" + column.isAutoIncremented() + " default=" + column.defaultValueExpression()
                + " enumValues=" + column.enumValues() + " comment=" + column.comment();
    }

    private static String col(String name, String metadata) {
        return "{\"name\":\"" + name + "\"," + metadata + "}";
    }

    private static String number(int precision, int scale) {
        return "\"type\":\"number\",\"precision\":" + precision + ",\"scale\":" + scale + ",\"nullable\":true";
    }

    private static String sized(String type, int length) {
        return "\"type\":\"" + type + "\",\"length\":" + length + ",\"nullable\":true";
    }

    private static String plain(String type) {
        return "\"type\":\"" + type + "\",\"nullable\":true";
    }
}
