package org.apache.flink.http.connectors.source.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.http.connectors.source.HttpSourceSplit;
import org.apache.flink.http.connectors.source.meta.CheckpointPosition;
import org.apache.flink.http.connectors.source.meta.RecordsAndPosition;
import org.apache.flink.http.connectors.source.params.HttpSourceParameters;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

public interface BulkFormat<T, SplitT extends HttpSourceSplit>
        extends Serializable, ResultTypeQueryable<T> {

    Reader<T> createReader(Configuration config, SplitT split, HttpSourceParameters parameters) throws IOException;

    Reader<T> restoreReader(Configuration config, SplitT split, HttpSourceParameters parameters, CheckpointPosition position) throws IOException;

    boolean isSplittable();

    @Override
    TypeInformation<T> getProducedType();

    // ------------------------------------------------------------------------

    /**
     * The actual reader that reads the batches of records.
     */
    interface Reader<T> extends Closeable {

        @Nullable
        RecordIterator<T> readBatch() throws IOException;
    }

    // ------------------------------------------------------------------------

    interface RecordIterator<T> {

        @Nullable
        RecordsAndPosition<T> next();

        void releaseBatch();
    }
}
