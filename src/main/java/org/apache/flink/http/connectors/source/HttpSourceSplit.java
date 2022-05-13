package org.apache.flink.http.connectors.source;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.http.connectors.source.meta.CheckpointPosition;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class HttpSourceSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 6348086794567295161L;

    private final String id;
    private final HttpSourceParameters parameters;

    @Nullable
    private CheckpointPosition position;

    /**
     * The splits are frequently serialized into checkpoints. Caching the byte representation makes
     * repeated serialization cheap. This field is used by {@link HttpSourceSplitSerializer}.
     */
    @Nullable
    transient byte[] serializedFormCache;

    public HttpSourceSplit(String id, HttpSourceParameters parameters) {
        this(id, parameters, null);
    }

    public HttpSourceSplit(String id, HttpSourceParameters parameters, CheckpointPosition position) {
        this(id, parameters, position, null);
    }

    public HttpSourceSplit(String id, HttpSourceParameters parameters, @Nullable CheckpointPosition position, @Nullable byte[] serializedFormCache) {
        this.id = id;
        this.parameters = checkNotNull(parameters);
        this.position = position;
        this.serializedFormCache = serializedFormCache;
    }

    @Override
    public String splitId() {
        return id;
    }

    public HttpSourceParameters getParameters() {
        return parameters;
    }

    public Optional<CheckpointPosition> getPosition() {
        return Optional.ofNullable(position);
    }

    public HttpSourceSplit updateWithCheckpointedPosition(CheckpointPosition position) {
        return new HttpSourceSplit(id, parameters, position);
    }
}
