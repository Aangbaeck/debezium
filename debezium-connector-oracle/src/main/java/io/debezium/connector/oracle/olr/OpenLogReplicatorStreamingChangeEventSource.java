/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnection.NonRelationalTableException;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleSchemaChangeEventEmitter;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.jdbc.OracleConnectionFactory;
import io.debezium.connector.oracle.olr.client.OlrNetworkClient;
import io.debezium.connector.oracle.olr.client.PayloadEvent;
import io.debezium.connector.oracle.olr.client.PayloadEvent.Type;
import io.debezium.connector.oracle.olr.client.StreamPosition;
import io.debezium.connector.oracle.olr.client.StreamingEvent;
import io.debezium.connector.oracle.olr.client.payloads.AbstractMutationEvent;
import io.debezium.connector.oracle.olr.client.payloads.PayloadSchema;
import io.debezium.connector.oracle.olr.client.payloads.SchemaChangeEvent;
import io.debezium.connector.oracle.olr.client.payloads.SchemaColumn;
import io.debezium.connector.oracle.olr.client.payloads.Values;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.OffsetActivityMonitorService;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

import oracle.sql.RAW;

/**
 * An implementation of {@link StreamingChangeEventSource} based on OpenLogReplicator.
 *
 * @author Chris Cranford
 */
public class OpenLogReplicatorStreamingChangeEventSource implements StreamingChangeEventSource<OraclePartition, OracleOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenLogReplicatorStreamingChangeEventSource.class);
    /** ORA-31603: object not found, from {@code DBMS_METADATA} when the table is dropped after it was looked up. */
    private static final int ORA_OBJECT_NOT_FOUND = 31603;

    private final OracleConnectorConfig connectorConfig;
    private final OracleConnectionFactory connectionFactory;
    private final EventDispatcher<OraclePartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final OpenLogReplicatorStreamingChangeEventSourceMetrics streamingMetrics;
    private final SnapshotterService snapshotterService;
    private final OffsetActivityMonitorService offsetActivityMonitorService;
    private final boolean confirmCompletedTransactions;

    private OlrNetworkClient client;
    private OraclePartition partition;
    private OracleOffsetContext offsetContext;
    private OffsetActivityMonitor<OraclePartition, OracleOffsetContext> offsetActivityMonitor;
    private boolean transactionEvents = false;
    /**
     * Whether a transaction's begin was read and its commit not yet. A checkpoint is never sent between the
     * two, this only keeps one from completing a transaction that is being read.
     */
    private boolean inTransaction = false;
    /**
     * Identifies the connection the completed position was reached on. A position from an earlier
     * connection may belong to a stream the server has numbered again since, after a START, so it
     * is only confirmed once this connection has vouched for it.
     */
    private volatile Long streamEpoch;
    /**
     * The position the connector had emitted up to when it last stopped.
     *
     * <p>Streaming resumes at the start of the transaction that position falls in, so the changes
     * before it arrive a second time and are discarded here rather than being filtered out by the
     * server. Cleared once the stream advances past that transaction.
     */
    private Scn replayScn;
    private Long replayScnIndex;
    private String replayTransactionId;

    public OpenLogReplicatorStreamingChangeEventSource(OracleConnectorConfig connectorConfig, OracleConnectionFactory connectionFactory,
                                                       EventDispatcher<OraclePartition, TableId> dispatcher,
                                                       ErrorHandler errorHandler, Clock clock,
                                                       OracleDatabaseSchema schema,
                                                       OpenLogReplicatorStreamingChangeEventSourceMetrics streamingMetrics, SnapshotterService snapshotterService) {
        this.connectorConfig = connectorConfig;
        this.dispatcher = dispatcher;
        this.connectionFactory = connectionFactory;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.streamingMetrics = streamingMetrics;
        this.snapshotterService = snapshotterService;
        this.offsetActivityMonitorService = OffsetActivityMonitorService.lookup(connectorConfig.getServiceRegistry());
        this.confirmCompletedTransactions = connectorConfig.isOpenLogReplicatorConfirmCompletedTransactions();
    }

    @Override
    public void init(OracleOffsetContext offsetContext) throws InterruptedException {
        this.offsetContext = offsetContext == null ? emptyContext() : offsetContext;
    }

    @Override
    public OracleOffsetContext getOffsetContext() {
        return this.offsetContext;
    }

    @Override
    public Optional<OffsetActivityMonitor<OraclePartition, OracleOffsetContext>> getOffsetActivityMonitor() {
        if (offsetActivityMonitor == null) {
            offsetActivityMonitor = new OpenLogReplicatorOffsetActivityMonitor(
                    connectorConfig.getOffsetActivityMonitorInterval(),
                    streamingMetrics);
        }
        return Optional.of(offsetActivityMonitor);
    }

    private OracleOffsetContext emptyContext() {
        return OracleOffsetContext.create().logicalName(connectorConfig)
                .snapshotPendingTransactions(Collections.emptyMap())
                .transactionContext(new TransactionContext())
                .incrementalSnapshotContext(new SignalBasedIncrementalSnapshotContext<>()).build();
    }

    @Override
    public void execute(ChangeEventSourceContext context, OraclePartition partition, OracleOffsetContext offsetContext) throws InterruptedException {

        try {
            this.partition = partition;
            this.offsetContext = offsetContext;

            final Scn startScn = connectorConfig.getAdapter().getOffsetScn(offsetContext);
            final Long offsetScnIndex = offsetContext.getScnIndex();

            // Resume at the start of the transaction that the offset falls in rather than at the
            // offset itself. OpenLogReplicator sends only what follows the requested position, so
            // asking it to resume mid-transaction relies on it cutting the stream in exactly the
            // right place. Replaying the transaction and discarding what was already emitted, the
            // way the LogMiner adapter does, removes that dependency.
            Long startScnIndex = offsetScnIndex;
            if (offsetScnIndex != null) {
                replayScn = startScn;
                replayScnIndex = offsetScnIndex;
                replayTransactionId = offsetContext.getTransactionId();
                startScnIndex = 0L;
                LOGGER.info("Replaying transaction {} at SCN {} from its start, skipping through index {}.",
                        replayTransactionId, replayScn, replayScnIndex);
            }
            inTransaction = false;

            final OlrNetworkClient newClient = new OlrNetworkClient(connectorConfig);
            final boolean connected = confirmCompletedTransactions
                    ? newClient.connectContinuingAfter(startScn, confirmed -> continuePosition(startScn, confirmed))
                    : newClient.connect(startScn, startScnIndex);
            if (connected) {
                if (confirmCompletedTransactions) {
                    onStreamStarted(startScn, newClient.getContinuedAfter());
                }
                this.client = newClient;
                try {
                    // Start read loop
                    while (client.isConnected() && context.isRunning()) {
                        final StreamingEvent event = client.readEvent();
                        if (event != null) {
                            onEvent(event);
                        }

                        offsetActivityMonitorService.pulse(partition, offsetContext);

                        if (context.isPaused()) {
                            LOGGER.info("Streaming will now pause");
                            context.streamingPaused();
                            context.waitSnapshotCompletion();
                            LOGGER.info("Streaming resumed");
                        }
                    }
                }
                finally {
                    try {
                        client.disconnect();
                        LOGGER.info("Client disconnected.");
                    }
                    catch (Exception e) {
                        LOGGER.error("Exception while disconnecting OpenLogReplicator client", e);
                    }
                }
            }
            else {
                LOGGER.warn("Failed to connect to OpenLogReplicator server.");
            }
        }
        catch (Exception e) {
            LOGGER.error("Failed: {}", e.getMessage(), e);
            errorHandler.setProducerThrowable(e);
        }
        finally {
            LOGGER.info("Streaming metrics dump: {}", streamingMetrics.toString());
            LOGGER.info("Offsets: {}", offsetContext);
        }
    }

    /**
     * The position a server that is already streaming continues after: the offset's completed
     * position, but no further than the position the server reports confirmed, and not before the
     * start of the offset SCN.
     *
     * <p>The stored completed position may come from before a START, which numbers the stream anew,
     * when no record was flushed after it. What the server reports confirmed belongs to its current
     * run and was confirmed from flushed offsets, so everything up to it has been delivered.
     *
     * @param startScn the offset SCN, never {@code null}
     * @param serverConfirmed the position the server reports confirmed, {@code null} if none
     */
    StreamPosition continuePosition(Scn startScn, StreamPosition serverConfirmed) {
        final StreamPosition start = new StreamPosition(startScn, 0L);
        if (serverConfirmed == null) {
            return start;
        }
        final StreamPosition completed = StreamPosition.resumeFrom(startScn,
                offsetContext.getCompletedScn(), offsetContext.getCompletedScnIndex());
        final StreamPosition position = StreamPosition.min(completed, serverConfirmed);
        return position.isAfter(start) ? position : start;
    }

    /**
     * Starts a new connection epoch. After a CONTINUE the completed position becomes the position the
     * server continues after; after a START it is dropped until the new stream completes one.
     * Positions stored by earlier connections carry another epoch and are confirmed as the start of
     * their offset SCN.
     *
     * @param startScn the offset SCN, never {@code null}
     * @param continuedAfter the position the server continues after, {@code null} after a START
     */
    void onStreamStarted(Scn startScn, StreamPosition continuedAfter) {
        streamEpoch = ThreadLocalRandom.current().nextLong();
        final StreamPosition start = new StreamPosition(startScn, 0L);
        if (continuedAfter != null && continuedAfter.isAfter(start)) {
            offsetContext.setCompletedPosition(continuedAfter.scn(), continuedAfter.index(), streamEpoch);
        }
        else {
            offsetContext.clearCompletedPosition();
        }
    }

    @Override
    public void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
        confirmCommittedScn(offset);
    }

    /**
     * Confirms the committed streaming position with OpenLogReplicator, allowing the server to
     * release everything before it.
     *
     * <p>The position has to come from the offset that was committed rather than from the last
     * event that was read. Reads run ahead of what has been delivered, so confirming a position
     * taken from the read loop releases changes the connector has not handed on yet, and those
     * changes are gone if it stops before they are. The connector task passes the offset Kafka
     * Connect has flushed, read back from offset storage, rather than that of the last record Kafka
     * acknowledged: acknowledgements on different topics arrive in any order, and a later record
     * acknowledged first would confirm records that may still fail to be written.
     *
     * <p>The offset SCN is the system change number of the transaction the connector was last
     * emitting changes from, so confirming it releases the transactions before that one and keeps
     * that transaction itself available to be replayed. Confirming a position within the
     * transaction would release the part of it that a restart has to read again.
     *
     * <p>Once the commit of a transaction, or a checkpoint read outside a transaction, has been
     * committed with the offset, everything up to its position has been emitted, and that position is
     * confirmed instead when it is further along. A server that is still streaming continues from it
     * after a restart; a server that starts again reads redo from the offset SCN, and the replay skips
     * the completed transaction by its identifier and index.
     *
     * <p>The completed position is used only with {@code openlogreplicator.confirm.completed.transactions}.
     * It relies on OpenLogReplicator stamping messages in commit order (c_scn never decreases along
     * the stream) and sending exact c_idx values, so that no message sent after a commit or a
     * checkpoint carries a lower position.
     *
     * @param offset the offset that has been committed, never {@code null}
     */
    private void confirmCommittedScn(Map<String, ?> offset) {
        if (client == null || !client.isConnected()) {
            return;
        }

        final Scn scn = OracleOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
        if (scn == null || scn.isNull()) {
            LOGGER.debug("Cannot flush latest offset SCN, no streaming position has been committed yet.");
            return;
        }

        final StreamPosition position = confirmPosition(offset);
        client.confirm(position.scn(), position.index());
    }

    /**
     * The position to confirm for a committed offset: its completed position when that belongs to the
     * stream being read and is past the start of the offset SCN, otherwise the start of the offset SCN.
     */
    StreamPosition confirmPosition(Map<String, ?> offset) {
        final Scn scn = OracleOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
        final Object epoch = offset.get(OracleOffsetContext.OLR_COMPLETED_EPOCH_KEY);
        if (!confirmCompletedTransactions || streamEpoch == null || !(epoch instanceof Number number) || number.longValue() != streamEpoch) {
            return new StreamPosition(scn, 0L);
        }
        final Object completedIndex = offset.get(OracleOffsetContext.OLR_COMPLETED_SCN_INDEX_KEY);
        return StreamPosition.resumeFrom(scn,
                OracleOffsetContext.getScnFromOffsetMapByKey(offset, OracleOffsetContext.OLR_COMPLETED_SCN_KEY),
                completedIndex instanceof Number index ? index.longValue() : null);
    }

    /**
     * Records that the stream has been emitted completely through the event's position, if that is
     * further along than the position recorded so far. The offset SCN is left where it is: it is
     * where a restart reads redo from, and a transaction still open in the database may have begun
     * before this position.
     */
    private void completeThrough(StreamingEvent event) {
        if (!confirmCompletedTransactions) {
            return;
        }
        final Scn scn = event.getCheckpointScn();
        final Long index = event.getCheckpointIndex();
        if (scn == null || scn.isNull() || index == null) {
            return;
        }
        final StreamPosition position = new StreamPosition(scn, index);
        final Scn completedScn = offsetContext.getCompletedScn();
        if (completedScn == null || position.isAfter(new StreamPosition(completedScn, offsetContext.getCompletedScnIndex()))) {
            offsetContext.setCompletedPosition(scn, index, streamEpoch);
        }
    }

    /**
     * Checks whether a change was already emitted before the connector restarted, and so arrived
     * only because streaming rewound to the start of the transaction it belongs to.
     *
     * @param event the event the change was read from, never {@code null}
     * @return {@code true} if the change should be discarded, {@code false} if it should be emitted
     */
    private boolean isAlreadyEmitted(StreamingEvent event) {
        if (replayScn == null) {
            return false;
        }

        final int comparison = event.getCheckpointScn().compareTo(replayScn);
        if (comparison > 0) {
            LOGGER.info("Replay completed, streaming resumes at SCN {}.", event.getCheckpointScn());
            replayScn = null;
            replayScnIndex = null;
            replayTransactionId = null;
            return false;
        }

        if (comparison < 0) {
            // Precedes the transaction being replayed, so it was emitted before the restart.
            return true;
        }

        return Objects.equals(event.getXid(), replayTransactionId)
                && event.getCheckpointIndex() <= replayScnIndex;
    }

    void onEvent(StreamingEvent event) throws Exception {
        for (PayloadEvent payloadEvent : event.getPayload()) {
            switch (payloadEvent.getType()) {
                case BEGIN:
                    onBeginEvent(event);
                    break;
                case COMMIT:
                    onCommitEvent(event);
                    break;
                case CHECKPOINT:
                    onCheckpointEvent(event);
                    break;
                case DDL:
                    onSchemaChangeEvent(event, (SchemaChangeEvent) payloadEvent);
                    break;
                case INSERT:
                case UPDATE:
                case DELETE:
                    onMutationEvent(event, (AbstractMutationEvent) payloadEvent);
                    break;
                default:
                    throw new DebeziumException("Unexpected event type detected: " + payloadEvent.getType());
            }
        }

        streamingMetrics.incrementProcessedEventsCount();
        streamingMetrics.setCheckpointDetails(event.getCheckpointScn(), event.getCheckpointIndex());
    }

    private void onBeginEvent(StreamingEvent event) {
        // The offset position is only advanced by changes that are dispatched, so that it always
        // describes something the connector has emitted. See #confirmCommittedScn.
        offsetContext.setEventScn(event.getCheckpointScn());
        offsetContext.setSourceTime(event.getTimestamp());
        transactionEvents = false;
        inTransaction = true;

        // We do not specifically start a transaction boundary here.
        //
        // This is delayed until the data change event on the first data change that is to be
        // captured by the connector in case there are transactions with events that are not
        // of interest to the connector.
    }

    private void onCommitEvent(StreamingEvent event) throws InterruptedException {
        offsetContext.setEventScn(event.getCheckpointScn());
        offsetContext.setSourceTime(event.getTimestamp());

        streamingMetrics.incrementCommittedTransactionCount();

        // Every message of the transaction has been read and its changes dispatched. Set before the
        // records below, which carry the offset that confirms it.
        inTransaction = false;
        completeThrough(event);

        // We may see empty transactions and in this case we don't want to emit a transaction boundary
        // record for these cases. Only trigger commit when there are valid changes.
        if (transactionEvents) {
            dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext, event.getTimestamp());
        }

        // For situations where capture tables are changed in-frequently, enabling heartbeats
        // will have a heartbeat emit at commit boundaries even if transaction metadata isn't
        // enabled to guarantee checkpoint offset flushes.
        dispatcher.alwaysDispatchHeartbeatEvent(partition, offsetContext);
    }

    private void onCheckpointEvent(StreamingEvent event) throws InterruptedException {
        // The offset SCN stays where it is: a restart reads redo from it, and a transaction that is
        // still open in the database, so not sent yet, may have begun before this marker.
        offsetContext.setEventScn(event.getCheckpointScn());
        offsetContext.setSourceTime(event.getTimestamp());

        // Everything sent before the marker has been read, so the stream is complete through it.
        if (!inTransaction) {
            completeThrough(event);
        }

        dispatcher.alwaysDispatchHeartbeatEvent(partition, offsetContext);
    }

    private void onMutationEvent(StreamingEvent event, AbstractMutationEvent mutationEvent) throws Exception {
        if (isAlreadyEmitted(event)) {
            LOGGER.trace("Skipping change at SCN {} index {}, it has already been emitted.",
                    event.getCheckpointScn(), event.getCheckpointIndex());
            return;
        }

        final Type eventType = mutationEvent.getType();
        final TableId tableId = mutationEvent.getSchema().getTableId(event.getDatabaseName());
        if (!connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
            return;
        }

        Table table = schema.tableFor(tableId);
        if (table == null) {
            Optional<Table> result = potentiallyEmitSchemaChangeForUnknownTable(eventType, tableId);
            if (result.isEmpty()) {
                return;
            }
            table = result.get();
        }
        else {
            table = refreshTableIfStale(event, mutationEvent, table);
        }

        final Operation operation;
        switch (eventType) {
            case INSERT:
                operation = Operation.CREATE;
                break;
            case UPDATE:
                operation = Operation.UPDATE;
                break;
            case DELETE:
                operation = Operation.DELETE;
                break;
            default:
                throw new DebeziumException("Unexpected DML event type: " + eventType);
        }

        // Update offsets. The position moves here, on a change that is about to be dispatched, so
        // that it always describes something the connector has emitted. The index identifies the
        // change within its transaction and is what a replay skips through on restart.
        offsetContext.setScn(event.getCheckpointScn());
        offsetContext.setScnIndex(event.getCheckpointIndex());
        // source.scn is the SCN of the row's own redo record when the message carries one (OpenLogReplicator
        // scn-type with the DML bit set), as the LogMiner adapter reports it; c_scn positions the stream and
        // remains the offset.
        offsetContext.setEventScn(event.getScn() != null && !event.getScn().isNull() ? event.getScn() : event.getCheckpointScn());
        offsetContext.setTransactionId(event.getXid());
        offsetContext.tableEvent(tableId, event.getTimestamp());
        offsetContext.setRowId(mutationEvent.getRid());

        streamingMetrics.setLastCapturedDmlCount(1);

        if (!transactionEvents) {
            // First data change that is of interest to the connector, emit the transaction start.
            dispatcher.dispatchTransactionStartedEvent(partition, event.getXid(), offsetContext, event.getTimestamp());
            transactionEvents = true;
        }

        final Object[] oldValues = toColumnValuesArray(table, mutationEvent.getBefore());
        final Object[] newValues = toColumnValuesArray(table, mutationEvent.getAfter());

        LOGGER.trace("Dispatching {} (SCN {}) for table {}", eventType, event.getScn(), tableId);
        dispatcher.dispatchDataChangeEvent(
                partition,
                tableId,
                new OpenLogReplicatorChangeRecordEmitter(
                        connectorConfig,
                        partition,
                        offsetContext,
                        operation,
                        oldValues,
                        newValues,
                        table,
                        schema,
                        clock));
    }

    private void onSchemaChangeEvent(StreamingEvent event, SchemaChangeEvent schemaEvent) throws Exception {
        if (isAlreadyEmitted(event)) {
            LOGGER.trace("Skipping schema change at SCN {} index {}, it has already been emitted.",
                    event.getCheckpointScn(), event.getCheckpointIndex());
            return;
        }

        final PayloadSchema payloadSchema = schemaEvent.getSchema();

        final TableId tableId = payloadSchema.getTableId(event.getDatabaseName());
        if (tableId.schema() == null || tableId.table().startsWith("OBJ_")) {
            LOGGER.trace("Cannot process DDL due to missing schema: {}", schemaEvent.getSql());
            return;
        }
        else if (tableId.table().startsWith("BIN$") && tableId.table().endsWith("==$0")) {
            LOGGER.trace("Skipping DDL for recycling bin table: {}", schemaEvent.getSql());
            return;
        }

        offsetContext.setScn(event.getCheckpointScn());
        offsetContext.setScnIndex(event.getCheckpointIndex());
        offsetContext.setEventScn(event.getCheckpointScn());
        offsetContext.setTransactionId(event.getXid());
        offsetContext.tableEvent(tableId, event.getTimestamp());

        final String sqlStatement = schemaEvent.getSql().toLowerCase().trim();

        // todo: do we want to let other ddl statements be emitted for non-tables?
        if (!isTableSqlStatement(sqlStatement)) {
            LOGGER.trace("Skipping internal DDL: {}", schemaEvent.getSql());
            return;
        }

        if (sqlStatement.contains("rename constraint ")) {
            LOGGER.trace("Ignoring constraint rename: {}", schemaEvent.getSql());
            return;
        }
        else if (sqlStatement.contains("rename to \"bin$")) {
            LOGGER.trace("Ignoring table rename to recycling object: {}", schemaEvent.getSql());
            return;
        }

        LOGGER.trace("Dispatching DDL (SCN {}): [{}]", event.getScn(), schemaEvent.getSql());
        dispatcher.dispatchSchemaChangeEvent(
                partition,
                offsetContext,
                tableId,
                new OracleSchemaChangeEventEmitter(
                        connectorConfig,
                        partition,
                        offsetContext,
                        tableId,
                        tableId.catalog(),
                        tableId.schema(),
                        schemaEvent.getSql(),
                        schema,
                        event.getTimestamp(),
                        streamingMetrics,
                        () -> processTruncateEvent(event, schemaEvent)));
    }

    private boolean isTableSqlStatement(String sqlStatement) {
        return sqlStatement.startsWith("create table ")
                || sqlStatement.startsWith("alter table ")
                || sqlStatement.startsWith("drop table ")
                || sqlStatement.startsWith("truncate table ");
    }

    private Object[] toColumnValuesArray(Table table, Values values) {
        Object[] results = new Object[table.columns().size()];
        if (values != null) {
            try {
                final TableId tableId = table.id();
                for (Column column : table.columns()) {
                    final int index = column.position() - 1;
                    final Object value = resolveColumnValue(tableId, column, values);
                    LOGGER.trace("Processing column at {} with name {} [jdbcType={}, type={},length={},scale={}] and value {} ({}).",
                            index, column.name(),
                            column.jdbcType(),
                            column.typeName(), column.length(), column.scale().orElse(0),
                            value, value != null ? value.getClass() : "<null>");
                    results[index] = value;
                }
            }
            catch (Exception e) {
                throw new DebeziumException("Failed to create column array values", e);
            }
        }
        return results;
    }

    /**
     * Reloads the table's model when the change does not fit it, which happens when the table was replaced under the
     * same name without a DDL for it, as {@code DBMS_REDEFINITION} does. The model is read from the database and then
     * adjusted to the change's column metadata, which describes the table as of the change: the table may have changed
     * again, or been dropped, before the connector reached the change.
     *
     * @return the model to emit the change with
     * @throws DebeziumException if no model can be built that fits the change
     */
    private Table refreshTableIfStale(StreamingEvent event, AbstractMutationEvent mutationEvent, Table table) throws Exception {
        final ColumnNameFilter columnFilter = connectorConfig.getColumnFilter();
        final String mismatch = findModelMismatch(table, mutationEvent, columnFilter);
        if (mismatch == null) {
            return table;
        }

        final TableId tableId = table.id();
        LOGGER.warn("The model of table {} does not match the change at SCN {} index {}: {}. Reloading it from the database.",
                tableId, event.getCheckpointScn(), event.getCheckpointIndex(), mismatch);

        // Positioned just before the change: a restart from an earlier transaction does not apply the new
        // model to the changes before it, and the record's offset does not mark the change as emitted.
        offsetContext.setScn(event.getCheckpointScn());
        offsetContext.setScnIndex(event.getCheckpointIndex() == null ? null : event.getCheckpointIndex() - 1);
        offsetContext.setEventScn(event.getCheckpointScn());
        offsetContext.setTransactionId(event.getXid());

        String databaseDdl = null;
        Table fromDatabase = null;
        try {
            databaseDdl = readTableDdl(tableId);
            fromDatabase = parseTable(tableId, databaseDdl);
        }
        catch (NonRelationalTableException e) {
            LOGGER.warn("{} Building the model of table {} as of the change from the change and the last known model.", e.getMessage(), tableId);
        }
        catch (SQLException e) {
            if (e.getErrorCode() != ORA_OBJECT_NOT_FOUND) {
                throw e;
            }
            LOGGER.warn("Table {} was dropped while its DDL was read. Building its model as of the change from the change and the last known model.", tableId);
        }

        final OpenLogReplicatorTableReconciler.Result reconciled;
        try {
            reconciled = OpenLogReplicatorTableReconciler.reconcile(fromDatabase != null ? fromDatabase : table, table, mutationEvent, columnFilter);
        }
        catch (OpenLogReplicatorTableReconciler.UnmappableColumnException e) {
            throw new DebeziumException(String.format("The model of table %s cannot be built for the change at SCN %s index %s: %s. "
                    + "The table changed after this change was written, and OpenLogReplicator's column metadata does not determine the "
                    + "type. Restore a schema history that describes the table as of this change, or skip the change.",
                    tableId, event.getCheckpointScn(), event.getCheckpointIndex(), e.getMessage()), e);
        }

        final String remaining = findModelMismatch(reconciled.table(), mutationEvent, columnFilter);
        if (remaining != null) {
            throw new DebeziumException(String.format(
                    "The model of table %s does not match the change at SCN %s index %s even after reloading it and applying the change's "
                            + "column metadata: %s. Restore a schema history that describes the table as of this change, or skip the change.",
                    tableId, event.getCheckpointScn(), event.getCheckpointIndex(), remaining));
        }
        if (reconciled.primaryKeyDropped()) {
            LOGGER.warn("A primary key column of table {} does not exist as of the change at SCN {}; the model has no primary key.",
                    tableId, event.getCheckpointScn());
        }
        if (!reconciled.changes().isEmpty()) {
            LOGGER.warn("The model of table {} as of the change at SCN {} index {} differs from the {}: {}. Recording it from the change's column metadata.",
                    tableId, event.getCheckpointScn(), event.getCheckpointIndex(), fromDatabase != null ? "database" : "last known model",
                    reconciled.changes());
        }

        final String ddl = fromDatabase != null && reconciled.changes().isEmpty() ? databaseDdl : OpenLogReplicatorTableReconciler.toDdl(reconciled.table());
        dispatchTableModel(reconciled.table(), ddl);
        return schema.tableFor(tableId);
    }

    /**
     * Compares a change with the model of its table.
     *
     * <p>A value for a column the model does not have, or a {@code NUMBER} column of the model that has another
     * type, precision or scale in the change, means the model describes another version of the table. Columns in the
     * change's schema but not among its values are hidden, unused or guard columns, which the model never has.
     *
     * @return a description of the first difference, or {@code null} if the change fits the model
     */
    static String findModelMismatch(Table table, AbstractMutationEvent mutationEvent, ColumnNameFilter columnFilter) {
        final TableId tableId = table.id();
        for (Values values : new Values[]{ mutationEvent.getBefore(), mutationEvent.getAfter() }) {
            if (values == null) {
                continue;
            }
            for (String name : values.getValues().keySet()) {
                if (table.columnWithName(name) == null
                        && (columnFilter == null || columnFilter.matches(tableId.catalog(), tableId.schema(), tableId.table(), name))) {
                    return "column " + name + " is not in the model";
                }
            }
        }

        final List<SchemaColumn> schemaColumns = mutationEvent.getSchema().getColumns();
        if (schemaColumns != null) {
            for (SchemaColumn schemaColumn : schemaColumns) {
                final Column column = table.columnWithName(schemaColumn.getName());
                if (column == null || !"NUMBER".equalsIgnoreCase(column.typeName())) {
                    continue;
                }
                if (!"number".equalsIgnoreCase(schemaColumn.getType())) {
                    return String.format("column %s is %s in the change and NUMBER in the model", column.name(), schemaColumn.getType());
                }
                if (schemaColumn.getPrecision() == null || schemaColumn.getPrecision() <= 0 || schemaColumn.getScale() == null
                        || column.length() <= 0) {
                    continue;
                }
                if (column.length() != schemaColumn.getPrecision() || column.scale().orElse(0) != schemaColumn.getScale().intValue()) {
                    return String.format("column %s is NUMBER(%d,%d) in the change and NUMBER(%d,%d) in the model",
                            column.name(), schemaColumn.getPrecision(), schemaColumn.getScale(), column.length(), column.scale().orElse(0));
                }
            }
        }
        return null;
    }

    /**
     * Reads a table's DDL in the PDB. The main connection stays in the root container while streaming, where a PDB's
     * tables are not visible.
     */
    private String readTableDdl(TableId tableId) throws SQLException {
        final OracleConnection connection = connectionFactory.mainConnection();
        final String pdbName = connectorConfig.getPdbName();
        if (Strings.isNullOrBlank(pdbName)) {
            return connection.getTableMetadataDdl(tableId);
        }
        connection.setSessionToPdb(pdbName);
        final String tableDdl;
        try {
            tableDdl = connection.getTableMetadataDdl(tableId);
        }
        catch (SQLException | RuntimeException e) {
            try {
                connection.resetSessionToCdb();
            }
            catch (RuntimeException resetFailure) {
                e.addSuppressed(resetFailure);
            }
            throw e;
        }
        connection.resetSessionToCdb();
        return tableDdl;
    }

    private Table parseTable(TableId tableId, String ddl) {
        final OracleDdlParser parser = schema.getDdlParser();
        parser.setCurrentDatabase(tableId.catalog());
        parser.setCurrentSchema(tableId.schema());
        final Tables tables = new Tables();
        try {
            parser.parse(ddl, tables);
        }
        catch (ParsingException | MultipleParsingExceptions e) {
            if (!schema.skipUnparseableDdlStatements()) {
                throw e;
            }
            LOGGER.warn("Ignoring unparsable DDL statement '{}':", ddl, e);
            streamingMetrics.incrementWarningCount();
            streamingMetrics.incrementSchemaChangeParseErrorCount();
            return null;
        }
        return tables.forTable(tableId);
    }

    private void dispatchTableModel(Table table, String ddl) throws InterruptedException {
        dispatcher.dispatchSchemaChangeEvent(partition, offsetContext, table.id(), receiver -> {
            offsetContext.tableEvent(table.id(), Instant.now());
            final io.debezium.schema.SchemaChangeEvent event = io.debezium.schema.SchemaChangeEvent.ofCreate(
                    partition, offsetContext, table.id().catalog(), table.id().schema(), ddl, table, false);
            if (!schema.skipSchemaChangeEvent(event)) {
                receiver.schemaChangeEvent(event);
            }
        });
    }

    private void dispatchTableSchemaFromDatabase(TableId tableId) throws Exception {
        final String tableDdl = readTableDdl(tableId);
        dispatcher.dispatchSchemaChangeEvent(
                partition,
                offsetContext,
                tableId,
                new OracleSchemaChangeEventEmitter(
                        connectorConfig,
                        partition,
                        offsetContext,
                        tableId,
                        tableId.catalog(),
                        tableId.schema(),
                        tableDdl,
                        schema,
                        Instant.now(),
                        streamingMetrics,
                        null));
    }

    private Optional<Table> potentiallyEmitSchemaChangeForUnknownTable(Type eventType, TableId tableId) throws Exception {
        if (!connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
            LOGGER.trace("{} for non-captured table {} detected.", eventType, tableId);
            return Optional.empty();
        }

        LOGGER.warn("Fetching schema for table {}, which should already be loaded. " +
                "This may indicate a potential error in your configuration.", tableId);
        try {
            dispatchTableSchemaFromDatabase(tableId);
        }
        catch (NonRelationalTableException e) {
            LOGGER.warn("{} The event will be skipped.", e.getMessage());
            streamingMetrics.incrementWarningCount();
            return Optional.empty();
        }

        return Optional.ofNullable(schema.tableFor(tableId));
    }

    private void processTruncateEvent(StreamingEvent event, SchemaChangeEvent ddlEvent) throws InterruptedException {
        if (ddlEvent.getSchema() == null) {
            LOGGER.warn("Truncate event ignored, no schema found.");
            return;
        }

        final TableId tableId = ddlEvent.getSchema().getTableId(event.getDatabaseName());
        if (!connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
            LOGGER.warn("Truncate event ignored, table is no included.");
            return;
        }

        Table table = schema.tableFor(tableId);
        if (table == null) {
            try {
                Optional<Table> result = potentiallyEmitSchemaChangeForUnknownTable(ddlEvent.getType(), tableId);
                if (result.isEmpty()) {
                    LOGGER.warn("Truncate ignored, cannot find table relational model");
                    return;
                }
                table = result.get();
            }
            catch (Exception e) {
                LOGGER.warn("Truncate ignored, failed to emit schema change", e);
                return;
            }
        }

        offsetContext.setScn(event.getScn());
        offsetContext.setEventScn(event.getScn());
        offsetContext.setTransactionId(event.getXid());
        offsetContext.tableEvent(tableId, event.getTimestamp());

        LOGGER.trace("Dispatching {} (SCN {}) for table {}", Operation.TRUNCATE, event.getScn(), tableId);
        dispatcher.dispatchDataChangeEvent(
                partition,
                tableId,
                new OpenLogReplicatorChangeRecordEmitter(
                        connectorConfig,
                        partition,
                        offsetContext,
                        Operation.TRUNCATE,
                        new Object[table.columns().size()],
                        new Object[table.columns().size()],
                        table,
                        schema,
                        clock));
    }

    private Object resolveColumnValue(TableId tableId, Column column, Values values) {
        Object value = values.getValues().getOrDefault(column.name(), OracleValueConverters.UNAVAILABLE_VALUE);
        if (value == OracleValueConverters.UNAVAILABLE_VALUE) {
            // If the get returned the unavailable value, the key does not exist.
            // If the column is LOB, return the unavailable value marker.
            // If the column is not an LOB, return null
            final List<Column> lobColumns = schema.getLobColumnsForTable(tableId);
            for (Column lobColumn : lobColumns) {
                if (lobColumn.equals(column)) {
                    return value;
                }
            }
            value = null;
        }
        else if (column.jdbcType() == Types.VARBINARY && value instanceof String stringValue) {
            // OpenLogReplicator sends binary columns hex encoded, as its payload is JSON and
            // cannot carry binary. The decode belongs here rather than in the value converter,
            // because a converter registered for a column through "converters" replaces the value
            // converter instead of running after it, and so would never see the decode.
            try {
                value = RAW.hexString2Bytes(stringValue);
            }
            catch (SQLException e) {
                throw new DebeziumException("Failed to convert HEX string into byte array: " + stringValue, e);
            }
        }
        return value;
    }

}
