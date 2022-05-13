package org.apache.flink.http.connectors.source.impl;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.http.connectors.source.HttpSourceSplit;
import org.apache.flink.http.connectors.source.HttpSourceSplitState;
import org.apache.flink.http.connectors.source.meta.RecordsAndPosition;

public class HttpSourceRecordEmitter<T, SplitT extends HttpSourceSplit>
        implements RecordEmitter<RecordsAndPosition<T>, T, HttpSourceSplitState<SplitT>> {

    @Override
    public void emitRecord(
            RecordsAndPosition<T> elements,
            SourceOutput<T> output,
            HttpSourceSplitState<SplitT> splitState) throws Exception {
        elements.getRecords().forEach(output::collect);
        splitState.setPosition(elements.getPosition());
    }
}
