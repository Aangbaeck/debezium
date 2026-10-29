/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.oracle.OracleConnectorConfig.ConnectorAdapter;
import io.debezium.connector.oracle.olr.OpenLogReplicatorOracleOffsetContextLoader;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.pipeline.ChangeEventSourceCoordinator;

/**
 * With the OpenLogReplicator adapter the offset handed on for confirmation is the one Kafka Connect flushed, read back
 * from offset storage, not the offset of the record Kafka acknowledged last: acknowledgements on different topics
 * arrive in any order, so a heartbeat acknowledged before the rows of its transaction must not confirm them.
 */
public class OracleConnectorTaskOlrCommitTest {

    @Test
    @SuppressWarnings("unchecked")
    void shouldConfirmFlushedOffsetNotLastAcknowledged() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(TestHelper.defaultConfig()
                .with(OracleConnectorConfig.OLR_SOURCE, "S1")
                .with(OracleConnectorConfig.OLR_HOST, "127.0.0.1")
                .with(OracleConnectorConfig.OLR_PORT, 1)
                .build());
        final OraclePartition partition = new OraclePartition(config.getLogicalName(), config.getDatabaseName());

        final Map<String, Object> flushed = new HashMap<>();
        flushed.put("scn", "100");
        flushed.put("scn_idx", 3L);
        final Map<String, Object> acknowledged = new HashMap<>();
        acknowledged.put("scn", "100");
        acknowledged.put("scn_idx", 3L);
        acknowledged.put(OracleOffsetContext.OLR_COMPLETED_SCN_KEY, "100");
        acknowledged.put(OracleOffsetContext.OLR_COMPLETED_SCN_INDEX_KEY, 4L);
        acknowledged.put(OracleOffsetContext.OLR_COMPLETED_BASE_SCN_KEY, "100");
        acknowledged.put(OracleOffsetContext.OLR_COMPLETED_EPOCH_KEY, 1L);

        final OffsetStorageReader reader = mock(OffsetStorageReader.class);
        when(reader.offsets(anyCollection())).thenAnswer(invocation -> {
            final Map<Map<String, String>, Map<String, Object>> result = new HashMap<>();
            for (Object key : (Collection<Object>) invocation.getArgument(0)) {
                result.put((Map<String, String>) key, flushed);
            }
            return result;
        });
        final SourceTaskContext context = mock(SourceTaskContext.class);
        when(context.offsetStorageReader()).thenReturn(reader);
        final ChangeEventSourceCoordinator<OraclePartition, OracleOffsetContext> coordinator = mock(ChangeEventSourceCoordinator.class);

        final OracleConnectorTask task = new OracleConnectorTask();
        task.initialize(context);
        set(task, OracleConnectorTask.class, "connectorAdapter", ConnectorAdapter.OLR);
        set(task, OracleConnectorTask.class, "partitionProvider", new OraclePartition.Provider(config));
        set(task, OracleConnectorTask.class, "offsetContextLoader", new OpenLogReplicatorOracleOffsetContextLoader(config));
        set(task, BaseSourceTask.class, "coordinator", coordinator);

        // the heartbeat of the commit was acknowledged, a row of the transaction not yet
        task.commitRecord(new SourceRecord(partition.getSourcePartition(), acknowledged, "heartbeat", null, null), null);
        task.performCommit();

        final ArgumentCaptor<Map<String, Object>> offset = ArgumentCaptor.forClass(Map.class);
        verify(coordinator).commitOffset(any(), offset.capture());
        assertThat((Map<String, Object>) offset.getValue()).containsEntry("scn", "100").containsEntry("scn_idx", 3L)
                .doesNotContainKey(OracleOffsetContext.OLR_COMPLETED_SCN_KEY);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldConfirmNothingBeforeAnythingWasFlushed() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(TestHelper.defaultConfig()
                .with(OracleConnectorConfig.OLR_SOURCE, "S1")
                .with(OracleConnectorConfig.OLR_HOST, "127.0.0.1")
                .with(OracleConnectorConfig.OLR_PORT, 1)
                .build());
        final OffsetStorageReader reader = mock(OffsetStorageReader.class);
        when(reader.offsets(anyCollection())).thenReturn(new HashMap<>());
        final SourceTaskContext context = mock(SourceTaskContext.class);
        when(context.offsetStorageReader()).thenReturn(reader);
        final ChangeEventSourceCoordinator<OraclePartition, OracleOffsetContext> coordinator = mock(ChangeEventSourceCoordinator.class);

        final OracleConnectorTask task = new OracleConnectorTask();
        task.initialize(context);
        set(task, OracleConnectorTask.class, "connectorAdapter", ConnectorAdapter.OLR);
        set(task, OracleConnectorTask.class, "partitionProvider", new OraclePartition.Provider(config));
        set(task, OracleConnectorTask.class, "offsetContextLoader", new OpenLogReplicatorOracleOffsetContextLoader(config));
        set(task, BaseSourceTask.class, "coordinator", coordinator);

        final Map<String, Object> acknowledged = new HashMap<>();
        acknowledged.put("scn", "100");
        task.commitRecord(new SourceRecord(new HashMap<>(), acknowledged, "heartbeat", null, null), null);
        task.performCommit();

        verify(coordinator, never()).commitOffset(any(), any());
    }

    private static void set(Object target, Class<?> type, String name, Object value) throws Exception {
        final Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
