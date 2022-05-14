package org.apache.flink.http.connectors.source.params;

import java.util.HashMap;
import java.util.Map;

public class SourceParameters {
    private Map<Parameters, String> paginationFields = new HashMap<>();
    private Map<Parameters, Long> paginationInitValues = new HashMap<>();

    public boolean isPaginationEnabled() {
        return paginationFields.size() > 0 || paginationInitValues.size() > 0;
    }

    private void parsePaginationFields() {

    }

    private void parsePaginationInitialValues() {

    }
}
