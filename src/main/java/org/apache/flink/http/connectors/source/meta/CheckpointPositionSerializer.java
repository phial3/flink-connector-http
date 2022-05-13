package org.apache.flink.http.connectors.source.meta;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.io.Serializable;

@FunctionalInterface
public interface CheckpointPositionSerializer extends Serializable {

    CheckpointPosition.Provider getPositionProvider();

    default CheckpointPosition readPosition(DataInputDeserializer in) throws IOException {
        CheckpointPosition.Provider positionFactory = getPositionProvider();
        CheckpointPosition position = positionFactory.create();
        position.read(in);
        return position;
    }

    default void writePosition(CheckpointPosition position, DataOutputSerializer out) throws IOException {
        position.write(out);
    }
}