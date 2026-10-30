/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client;

import io.debezium.connector.oracle.Scn;

/**
 * A position in the OpenLogReplicator stream, ordered by c_scn and then c_idx.
 *
 * @param scn the c_scn, never {@code null}
 * @param index the c_idx
 */
public record StreamPosition(Scn scn, long index) {

    /**
     * The position to confirm and to continue from: the start of the offset SCN, or the completed
     * position when that is further along.
     *
     * @param scn the offset SCN, never {@code null}
     * @param completedScn the c_scn of the completed position, may be {@code null}
     * @param completedIndex the c_idx of the completed position, may be {@code null}
     */
    public static StreamPosition resumeFrom(Scn scn, Scn completedScn, Long completedIndex) {
        final StreamPosition start = new StreamPosition(scn, 0L);
        if (completedScn == null || completedScn.isNull()) {
            return start;
        }
        final StreamPosition completed = new StreamPosition(completedScn, completedIndex != null ? completedIndex : 0L);
        return completed.isAfter(start) ? completed : start;
    }

    public boolean isAfter(StreamPosition other) {
        final int comparison = scn.compareTo(other.scn);
        return comparison > 0 || (comparison == 0 && index > other.index);
    }

    public static StreamPosition min(StreamPosition a, StreamPosition b) {
        return a.isAfter(b) ? b : a;
    }
}
