/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ProtocolMessageEnum;

import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RequestCode;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.ResponseCode;

/**
 * The request and response codes are wire values shared with OpenLogReplicator's own copy of
 * {@code OraProtoBuf.proto}. A change here must be made on both sides, or the client misreads every answer.
 */
public class OlrProtocolCodesTest {

    @Test
    public void shouldKeepRequestCodes() {
        assertThat(numbers(RequestCode.values())).containsExactlyInAnyOrderEntriesOf(Map.of(
                "INFO", 0,
                "START", 1,
                "CONTINUE", 2,
                "CONFIRM", 3));
    }

    @Test
    public void shouldKeepResponseCodes() {
        final Map<String, Integer> expected = new LinkedHashMap<>();
        expected.put("READY", 0);
        expected.put("FAILED_START", 1);
        expected.put("STARTING", 2);
        expected.put("ALREADY_STARTED", 3);
        expected.put("REPLICATE", 4);
        expected.put("PAYLOAD", 5);
        expected.put("INVALID_DATABASE", 6);
        expected.put("INVALID_COMMAND", 7);
        assertThat(numbers(ResponseCode.values())).containsExactlyInAnyOrderEntriesOf(expected);
    }

    private static Map<String, Integer> numbers(Enum<?>[] values) {
        final Map<String, Integer> numbers = new LinkedHashMap<>();
        Arrays.stream(values)
                .filter(value -> !"UNRECOGNIZED".equals(value.name()))
                .forEach(value -> numbers.put(value.name(), ((ProtocolMessageEnum) value).getNumber()));
        return numbers;
    }
}
