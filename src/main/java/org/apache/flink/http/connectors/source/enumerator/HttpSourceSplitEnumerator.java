package org.apache.flink.http.connectors.source.enumerator;


import org.apache.flink.http.connectors.source.HttpSourceSplit;
import org.apache.flink.http.connectors.source.HttpSourceParameters;

import java.io.Serializable;
import java.util.Collection;

public interface HttpSourceSplitEnumerator<SplitT extends HttpSourceSplit> {

    Collection<SplitT> enumerateSplits(HttpSourceParameters parameters);

    /**
     * Factory for the {@code HttpSourceEnumerator}.
     */
    @FunctionalInterface
    interface Provider extends Serializable {

        HttpSourceSplitEnumerator create();
    }
}
