package org.apache.flink.http.connectors.source.meta;

import lombok.Getter;

import java.util.Collection;

@Getter
public class RecordsAndPosition<E> {

    private final Collection<E> records;
    private final CheckpointPosition position;

    public RecordsAndPosition(Collection<E> records, CheckpointPosition position) {
        this.records = records;
        this.position = position;
    }
}
