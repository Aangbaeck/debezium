/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.relational.Column;

import oracle.jdbc.OracleTypes;

/**
 * OpenLogReplicator sends TIMESTAMP WITH TIME ZONE as "<epoch nanos>,<zone>", where the zone is a region id or an offset.
 */
public class OpenLogReplicatorValueConverterTimeZoneTest {

    private static final long EPOCH_NANOS = Instant.parse("2026-07-01T12:00:00Z").getEpochSecond() * 1_000_000_000L;

    private final OpenLogReplicatorValueConverter converter = new OpenLogReplicatorValueConverter(
            new OracleConnectorConfig(TestHelper.defaultConfig().build()), Mockito.mock(OracleConnection.class));
    private final Column column = Column.editor().name("TS").type("TIMESTAMP(6) WITH TIME ZONE")
            .jdbcType(OracleTypes.TIMESTAMPTZ).scale(6).create();
    private final Field field = new Field("TS", 0, Schema.OPTIONAL_STRING_SCHEMA);

    @Test
    public void shouldConvertKnownRegion() {
        assertThat(convert(EPOCH_NANOS + ",Europe/Stockholm")).isEqualTo("2026-07-01T14:00:00.000000+02:00");
    }

    @Test
    public void shouldConvertOffset() {
        assertThat(convert(EPOCH_NANOS + ",-05:00")).isEqualTo("2026-07-01T07:00:00.000000-05:00");
    }

    @Test
    public void shouldConvertGmt() {
        assertThat(convert(EPOCH_NANOS + ",GMT")).isEqualTo("2026-07-01T12:00:00.000000+00:00");
    }

    @ParameterizedTest
    @CsvSource({
            "ROC, 2026-07-01T20:00:00.000000+08:00",
            "US/Pacific-New, 2026-07-01T05:00:00.000000-07:00",
            "Canada/East-Saskatchewan, 2026-07-01T06:00:00.000000-06:00"
    })
    public void shouldConvertOracleRegionUnknownToJava(String region, String expected) {
        assertThat(convert(EPOCH_NANOS + "," + region)).isEqualTo(expected);
    }

    @Test
    public void shouldFailOnUnknownRegion() {
        assertThatThrownBy(() -> convert(EPOCH_NANOS + ",Mars/Olympus_Mons"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Mars/Olympus_Mons");
    }

    @Test
    public void shouldFailOnInvalidOffset() {
        assertThatThrownBy(() -> convert(EPOCH_NANOS + ",+25:00"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining(EPOCH_NANOS + ",+25:00");
    }

    private Object convert(String value) {
        return converter.convertTimestampWithZone(column, field, value);
    }
}
