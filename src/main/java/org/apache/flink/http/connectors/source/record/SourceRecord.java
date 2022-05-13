package org.apache.flink.http.connectors.source.record;

import java.util.Date;

public class SourceRecord {

    private boolean success;
    private int code;
    private String message;
    private Throwable throwable;

    private Boolean hasNext;
    private Integer pageIndex;
    private Integer pageSize;

    private Date startTime;
    private Date endTime;
}
