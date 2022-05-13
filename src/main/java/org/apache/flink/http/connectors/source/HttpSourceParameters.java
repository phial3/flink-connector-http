package org.apache.flink.http.connectors.source;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;

public class HttpSourceParameters implements IOReadableWritable, Serializable {

    public Object[] getParameters() {
        return null;
    }
    public Object[] getQuerys() {
        return null;
    }
    public Object[] getBodys() {
        return null;
    }
    public Object[] getHeaders() {
        return null;
    }

    @Override
    public void write(DataOutputView out) throws IOException {

    }

    @Override
    public void read(DataInputView in) throws IOException {

    }
}
