package org.apache.flink.http.connectors.source.params;

import lombok.Getter;
import lombok.Setter;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serializable;
import java.util.Map;

@Getter
@Setter
public class HttpSourceParameters implements Serializable {

    private String urlTemplate;
    private ObjectNode parameters;
    private Map<String, String> headers;
    private ObjectNode query;
    private ObjectNode payloads;

    public HttpSourceParameters() {
    }

    public HttpSourceParameters(String urlTemplate, ObjectNode parameters, Map<String, String> headers, ObjectNode query, ObjectNode payloads) {
        this.urlTemplate = urlTemplate;
        this.parameters = parameters;
        this.headers = headers;
        this.query = query;
        this.payloads = payloads;
    }
}
