/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import java.util.Map;

import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;

/**
 * The {@link OffsetContext} loader implementation for OpenLogReplicator.
 *
 * @author Chris Cranford
 */
public class OpenLogReplicatorOracleOffsetContextLoader implements OffsetContext.Loader<OracleOffsetContext> {

    private final OracleConnectorConfig connectorConfig;

    public OpenLogReplicatorOracleOffsetContextLoader(OracleConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public OracleOffsetContext load(Map<String, ?> offset) {
        return OracleOffsetContext.create()
                .logicalName(connectorConfig)
                .scn(OracleOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY))
                .scnIndex((Long) offset.get(SourceInfo.SCN_INDEX_KEY))
                .transactionId(OracleOffsetContext.loadTransactionId(offset))
                .commitScn(CommitScn.empty())
                .snapshot(loadSnapshot(offset).orElse(null))
                .snapshotCompleted(loadSnapshotCompleted(offset))
                .transactionContext(TransactionContext.load(offset))
                .incrementalSnapshotContext(SignalBasedIncrementalSnapshotContext.load(offset))
                .completedPosition(completedScn(offset), longValue(offset, OracleOffsetContext.OLR_COMPLETED_SCN_INDEX_KEY),
                        OracleOffsetContext.getScnFromOffsetMapByKey(offset, OracleOffsetContext.OLR_COMPLETED_BASE_SCN_KEY),
                        longValue(offset, OracleOffsetContext.OLR_COMPLETED_EPOCH_KEY))
                .build();
    }

    /**
     * The completed position's c_scn, or {@code null} when the position cannot be trusted: it has no
     * base SCN, or the offset SCN is below that base. The offset SCN does not move back while
     * streaming, so a lower one means the offset was rewound and the position belongs to a stream
     * the connector will read again.
     */
    private static Scn completedScn(Map<String, ?> offset) {
        final Scn completedScn = OracleOffsetContext.getScnFromOffsetMapByKey(offset, OracleOffsetContext.OLR_COMPLETED_SCN_KEY);
        if (completedScn == null || completedScn.isNull()) {
            return null;
        }
        final Scn baseScn = OracleOffsetContext.getScnFromOffsetMapByKey(offset, OracleOffsetContext.OLR_COMPLETED_BASE_SCN_KEY);
        final Scn scn = OracleOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
        if (baseScn == null || scn == null || scn.compareTo(baseScn) < 0) {
            return null;
        }
        return completedScn;
    }

    private static Long longValue(Map<String, ?> offset, String key) {
        final Object value = offset.get(key);
        return value instanceof Number number ? number.longValue() : null;
    }

}
