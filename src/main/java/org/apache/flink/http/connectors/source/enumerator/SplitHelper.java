package org.apache.flink.http.connectors.source.enumerator;

import org.apache.flink.http.connectors.source.HttpSourceSplit;
import org.apache.flink.http.connectors.source.meta.CheckpointPosition;
import org.apache.flink.http.connectors.source.params.HttpSourceParameters;

import java.util.Collection;
import java.util.function.Supplier;

public interface SplitHelper<SplitT extends HttpSourceSplit> {

    Collection<SplitT> split(Supplier<String> splitIdCreator, HttpSourceParameters parameters);

    CheckpointPosition toCheckpoint(HttpSourceParameters parameters);
}
