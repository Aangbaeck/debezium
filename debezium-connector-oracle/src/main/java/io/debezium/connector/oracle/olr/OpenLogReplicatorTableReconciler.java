/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.olr.client.payloads.AbstractMutationEvent;
import io.debezium.connector.oracle.olr.client.payloads.SchemaColumn;
import io.debezium.connector.oracle.olr.client.payloads.Values;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.Tables.ColumnNameFilter;

import oracle.jdbc.OracleTypes;

/**
 * Adjusts a table model to the column metadata of an OpenLogReplicator change, which describes the table as of the
 * change. Used only when a change does not fit the model and the model read from the database does not fit it either.
 */
final class OpenLogReplicatorTableReconciler {

    /** OpenLogReplicator sends a {@code NULL} precision or scale as -1. */
    private static final int OLR_UNSET = -1;
    /** Byte length of a {@code TIMESTAMP(0)}; every other fractional precision is stored in 11 bytes. */
    private static final int TIMESTAMP_WITHOUT_FRACTION_LENGTH = 7;
    private static final int NUMBER_MAX_PRECISION = 38;

    /**
     * A column whose type cannot be derived from OpenLogReplicator's metadata without guessing.
     */
    static final class UnmappableColumnException extends DebeziumException {
        UnmappableColumnException(String message) {
            super(message);
        }
    }

    /**
     * @param table the reconciled model
     * @param changes what differs from the base model, empty if nothing
     * @param primaryKeyDropped whether the base model's primary key was dropped because a column of it no longer exists
     */
    record Result(Table table, List<String> changes, boolean primaryKeyDropped) {
    }

    private OpenLogReplicatorTableReconciler() {
    }

    /**
     * Drops the columns of {@code base} that the change's column list does not have, replaces those whose type differs,
     * and adds those that have a value in the change but are not in {@code base}. A replaced or added column is taken
     * from {@code lastKnown} if its type there matches, and otherwise mapped from the change's metadata.
     *
     * @param base the model read from the database, or the last known model if the table no longer exists
     * @param lastKnown the model the change was first compared with, may be {@code null}
     * @throws UnmappableColumnException if a column has to be built from a type that cannot be mapped
     */
    static Result reconcile(Table base, Table lastKnown, AbstractMutationEvent change, ColumnNameFilter columnFilter) {
        final List<SchemaColumn> schemaColumns = change.getSchema().getColumns();
        if (schemaColumns == null || schemaColumns.isEmpty()) {
            return new Result(base, Collections.emptyList(), false);
        }
        final Map<String, SchemaColumn> byName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        schemaColumns.forEach(c -> byName.put(c.getName(), c));

        final List<String> changes = new ArrayList<>();
        final List<Column> columns = new ArrayList<>();
        for (Column column : base.columns()) {
            final SchemaColumn schemaColumn = byName.get(column.name());
            if (schemaColumn == null) {
                // virtual columns are not stored, so never in the list
                if (column.isGenerated()) {
                    columns.add(column);
                }
                else {
                    changes.add("dropped " + column.name());
                }
            }
            else if (isCompatible(column, schemaColumn)) {
                // only relaxed: a null the model does not allow fails the change, a NOT NULL it lacks does not
                if (!column.isOptional() && schemaColumn.isNullable()) {
                    columns.add(column.edit().optional(true).create());
                    changes.add("made nullable " + column.name());
                }
                else {
                    columns.add(column);
                }
            }
            else {
                columns.add(fromLastKnownOrMetadata(base, lastKnown, schemaColumn));
                changes.add("changed type of " + column.name() + " to " + describe(schemaColumn));
            }
        }

        for (Values values : new Values[]{ change.getBefore(), change.getAfter() }) {
            if (values == null) {
                continue;
            }
            for (String name : values.getValues().keySet()) {
                if (columns.stream().anyMatch(c -> c.name().equalsIgnoreCase(name))
                        || (columnFilter != null && !columnFilter.matches(base.id().catalog(), base.id().schema(), base.id().table(), name))) {
                    continue;
                }
                final SchemaColumn schemaColumn = byName.get(name);
                if (schemaColumn == null) {
                    throw new UnmappableColumnException("column " + name + " has a value but is not in the change's column list");
                }
                columns.add(fromLastKnownOrMetadata(base, lastKnown, schemaColumn));
                changes.add("added " + name + " " + describe(schemaColumn));
            }
        }

        if (changes.isEmpty()) {
            return new Result(base, changes, false);
        }

        final List<String> primaryKey = base.primaryKeyColumnNames();
        final boolean keepPrimaryKey = primaryKey.stream().allMatch(pk -> columns.stream().anyMatch(c -> c.name().equals(pk)));
        final TableEditor editor = Table.editor()
                .tableId(base.id())
                .setComment(base.comment())
                .setDefaultCharsetName(base.defaultCharsetName())
                .addAttributes(base.attributes());
        columns.forEach(editor::addColumns);
        if (keepPrimaryKey) {
            editor.setPrimaryKeyNames(primaryKey);
        }
        return new Result(editor.create(), changes, !keepPrimaryKey);
    }

    private static Column fromLastKnownOrMetadata(Table base, Table lastKnown, SchemaColumn schemaColumn) {
        final Column known = lastKnown == null ? null : lastKnown.columnWithName(schemaColumn.getName());
        if (known != null && isCompatible(known, schemaColumn)) {
            return known.isOptional() || !schemaColumn.isNullable() ? known : known.edit().optional(true).create();
        }
        return toColumn(base, schemaColumn);
    }

    /**
     * Whether a model column can hold the values of a column OpenLogReplicator describes. Compares what the metadata
     * states exactly: the type family, a {@code NUMBER}'s precision and scale, a {@code RAW}'s length, and whether a
     * {@code TIMESTAMP} has a fraction. Character lengths are in bytes in the metadata and not compared.
     */
    static boolean isCompatible(Column column, SchemaColumn schemaColumn) {
        final String type = lower(schemaColumn.getType());
        final String typeName = normalizedTypeName(column);
        final int precision = valueOrUnset(schemaColumn.getPrecision());
        final int scale = valueOrUnset(schemaColumn.getScale());
        switch (type) {
            case "number":
                if ("FLOAT".equals(typeName)) {
                    return precision > 0 && scale == OLR_UNSET && column.length() == precision;
                }
                if (!"NUMBER".equals(typeName)) {
                    return false;
                }
                if (precision > 0) {
                    return column.length() == precision && column.scale().isPresent() && column.scale().get() == scale;
                }
                // NUMBER or NUMBER(*,s); a model read over JDBC has length 0, one parsed from DDL has 38
                return (column.length() <= 0 || column.length() == NUMBER_MAX_PRECISION)
                        && (scale == OLR_UNSET ? column.scale().isEmpty() : column.scale().equals(Optional.of(scale)));
            case "varchar2":
                return "VARCHAR2".equals(typeName) || "NVARCHAR2".equals(typeName);
            case "char":
                return "CHAR".equals(typeName) || "NCHAR".equals(typeName);
            case "clob":
                return "CLOB".equals(typeName) || "NCLOB".equals(typeName);
            case "raw":
                return "RAW".equals(typeName) && schemaColumn.getLength() != null && column.length() == schemaColumn.getLength();
            case "timestamp":
                return "TIMESTAMP".equals(typeName) && hasFraction(column) == hasFraction(schemaColumn);
            case "unknown":
                // nothing to compare with
                return true;
            default:
                return typeName.equalsIgnoreCase(type);
        }
    }

    /**
     * Maps OpenLogReplicator's description of a column to the column {@code OracleDdlParser} builds for the same type
     * from {@code DBMS_METADATA} DDL.
     *
     * @throws UnmappableColumnException if the metadata does not determine the column, e.g. a {@code TIMESTAMP}'s
     *         fractional precision
     */
    static Column toColumn(Table table, SchemaColumn schemaColumn) {
        final ColumnEditor editor = Column.editor().name(schemaColumn.getName()).optional(schemaColumn.isNullable());
        final int precision = valueOrUnset(schemaColumn.getPrecision());
        final int scale = valueOrUnset(schemaColumn.getScale());
        switch (lower(schemaColumn.getType())) {
            case "number":
                if (precision > 0 && scale == OLR_UNSET) {
                    // the dictionary has no scale for FLOAT; NUMBER(p,-1) is reported the same way
                    return editor.jdbcType(Types.FLOAT).type("FLOAT").length(precision).create();
                }
                if (precision > 0) {
                    return editor.jdbcType(Types.NUMERIC).type("NUMBER").length(precision).scale(scale).create();
                }
                if (precision == OLR_UNSET) {
                    editor.jdbcType(Types.NUMERIC).type("NUMBER").length(NUMBER_MAX_PRECISION);
                    return scale == OLR_UNSET ? editor.create() : editor.scale(scale).create();
                }
                break;
            case "binary_float":
                return editor.jdbcType(OracleTypes.BINARY_FLOAT).type("BINARY_FLOAT").create();
            case "binary_double":
                return editor.jdbcType(OracleTypes.BINARY_DOUBLE).type("BINARY_DOUBLE").create();
            case "varchar2":
                // NVARCHAR2 and character length semantics are not told apart; the length is in bytes
                if (schemaColumn.getLength() != null) {
                    return editor.jdbcType(Types.VARCHAR).type("VARCHAR2").length(schemaColumn.getLength()).create();
                }
                break;
            case "char":
                if (schemaColumn.getLength() != null) {
                    return editor.jdbcType(Types.CHAR).type("CHAR").length(schemaColumn.getLength()).create();
                }
                break;
            case "raw":
                if (schemaColumn.getLength() != null) {
                    return editor.jdbcType(OracleTypes.RAW).type("RAW").length(schemaColumn.getLength()).create();
                }
                break;
            case "date":
                return editor.jdbcType(Types.TIMESTAMP).type("DATE").create();
            case "timestamp":
                if (!hasFraction(schemaColumn)) {
                    return editor.jdbcType(Types.TIMESTAMP).type("TIMESTAMP").length(0).create();
                }
                break;
            case "clob":
                // NCLOB is reported as clob
                return editor.jdbcType(Types.CLOB).type("CLOB").create();
            case "blob":
                return editor.jdbcType(Types.BLOB).type("BLOB").create();
            default:
                break;
        }
        throw new UnmappableColumnException(String.format("column %s of table %s is %s in the change, which does not determine its type in the model",
                schemaColumn.getName(), table.id(), describe(schemaColumn)));
    }

    /**
     * Renders a model as a {@code CREATE TABLE} that {@code OracleDdlParser} parses back to the same columns and key.
     */
    static String toDdl(Table table) {
        final StringBuilder ddl = new StringBuilder("CREATE TABLE ").append(quote(table.id().schema())).append('.').append(quote(table.id().table())).append(" (");
        ddl.append(table.columns().stream().map(OpenLogReplicatorTableReconciler::columnDefinition).collect(Collectors.joining(", ")));
        if (!table.primaryKeyColumnNames().isEmpty()) {
            ddl.append(", PRIMARY KEY (")
                    .append(table.primaryKeyColumnNames().stream().map(OpenLogReplicatorTableReconciler::quote).collect(Collectors.joining(", ")))
                    .append(')');
        }
        return ddl.append(");").toString();
    }

    private static String columnDefinition(Column column) {
        final StringBuilder definition = new StringBuilder(quote(column.name())).append(' ').append(typeDefinition(column));
        column.defaultValueExpression().ifPresent(expression -> definition.append(" DEFAULT ").append(expression));
        if (!column.isOptional()) {
            definition.append(" NOT NULL");
        }
        return definition.toString();
    }

    private static String typeDefinition(Column column) {
        final String typeName = normalizedTypeName(column);
        switch (typeName) {
            case "NUMBER":
                if (column.scale().isEmpty()) {
                    return "NUMBER";
                }
                return column.length() > 0 ? "NUMBER(" + column.length() + "," + column.scale().get() + ")" : "NUMBER(*," + column.scale().get() + ")";
            case "FLOAT":
            case "VARCHAR2":
            case "NVARCHAR2":
            case "CHAR":
            case "NCHAR":
            case "RAW":
                return column.length() > 0 ? typeName + "(" + column.length() + ")" : typeName;
            case "TIMESTAMP":
                return "TIMESTAMP(" + timePrecision(column) + ")";
            case "TIMESTAMP WITH TIME ZONE":
                return "TIMESTAMP(" + timePrecision(column) + ") WITH TIME ZONE";
            case "TIMESTAMP WITH LOCAL TIME ZONE":
                return "TIMESTAMP(" + timePrecision(column) + ") WITH LOCAL TIME ZONE";
            case "INTERVAL YEAR TO MONTH":
                return "INTERVAL YEAR(" + column.length() + ") TO MONTH";
            case "INTERVAL DAY TO SECOND":
                return "INTERVAL DAY(" + column.length() + ") TO SECOND(" + column.scale().orElse(6) + ")";
            default:
                return column.typeName();
        }
    }

    private static int timePrecision(Column column) {
        final int precision = column.scale().orElse(column.length());
        return precision < 0 ? 6 : precision;
    }

    private static boolean hasFraction(Column column) {
        return timePrecision(column) > 0;
    }

    private static boolean hasFraction(SchemaColumn schemaColumn) {
        return schemaColumn.getLength() == null || schemaColumn.getLength() != TIMESTAMP_WITHOUT_FRACTION_LENGTH;
    }

    /** The type name without a precision, as a model read over JDBC has it ({@code TIMESTAMP(6) WITH TIME ZONE}). */
    private static String normalizedTypeName(Column column) {
        return column.typeName() == null ? "" : column.typeName().replaceAll("\\(\\d+\\)", "").toUpperCase(Locale.ROOT);
    }

    static String describe(SchemaColumn schemaColumn) {
        final StringBuilder description = new StringBuilder(String.valueOf(schemaColumn.getType()));
        if (schemaColumn.getPrecision() != null || schemaColumn.getScale() != null) {
            description.append('(').append(schemaColumn.getPrecision()).append(',').append(schemaColumn.getScale()).append(')');
        }
        else if (schemaColumn.getLength() != null) {
            description.append(" length ").append(schemaColumn.getLength());
        }
        return description.toString();
    }

    private static String quote(String identifier) {
        return '"' + identifier + '"';
    }

    private static String lower(String value) {
        return value == null ? "" : value.toLowerCase(Locale.ROOT);
    }

    private static int valueOrUnset(Integer value) {
        return value == null ? OLR_UNSET : value;
    }
}
