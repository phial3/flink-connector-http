package org.apache.flink.http.connectors.source.enumerator;


import org.apache.flink.http.connectors.source.HttpSourceSplit;
import org.apache.flink.http.connectors.source.params.HttpSourceParameters;

import java.util.Collection;

public class HttpSourceSplitEnumeratorImpl<SplitT extends HttpSourceSplit>
        implements HttpSourceSplitEnumerator<SplitT> {

    /**
     * The current Id as a mutable string representation. This covers more values than the integer
     * value range, so we should never overflow.
     */
    private final char[] currentId = "0000000000".toCharArray();

    private final SplitHelper<SplitT> helper;

    public HttpSourceSplitEnumeratorImpl(SplitHelper<SplitT> helper) {
        this.helper = helper;
    }

    @Override
    public Collection<SplitT> enumerateSplits(HttpSourceParameters parameters) {
        return helper.split(() -> getNextId(), parameters);
    }

    protected final String getNextId() {
        // because we just increment numbers, we increment the char representation directly,
        // rather than incrementing an integer and converting it to a string representation
        // every time again (requires quite some expensive conversion logic).
        incrementCharArrayByOne(currentId, currentId.length - 1);
        return new String(currentId);
    }

    private static void incrementCharArrayByOne(char[] array, int pos) {
        char c = array[pos];
        c++;

        if (c > '9') {
            c = '0';
            incrementCharArrayByOne(array, pos - 1);
        }
        array[pos] = c;
    }
}
