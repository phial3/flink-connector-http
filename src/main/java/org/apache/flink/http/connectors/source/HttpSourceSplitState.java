package org.apache.flink.http.connectors.source;

import org.apache.flink.http.connectors.source.meta.CheckpointPosition;
import org.apache.flink.http.connectors.util.Rethrower;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

public class HttpSourceSplitState<SplitT extends HttpSourceSplit> {

    private final SplitT split;
    private CheckpointPosition position;

    public HttpSourceSplitState(SplitT split) {
        this.split = split;
        this.position = split.getPosition().orElse(null);
    }

    public CheckpointPosition getPosition() {
        return position;
    }

    public void setPosition(CheckpointPosition position) {
        this.position = position;
    }

    public SplitT toSourceSplit() {
        try {
            CheckpointPosition copyed = InstantiationUtil.clone(position);
            return (SplitT) split.updateWithCheckpointedPosition(copyed);
        } catch (IOException | ClassNotFoundException e) {
            Rethrower.throwAs(e);
        }
        return null;
    }
}
