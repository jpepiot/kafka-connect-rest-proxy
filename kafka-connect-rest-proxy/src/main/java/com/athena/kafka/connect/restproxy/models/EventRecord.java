package com.athena.kafka.connect.restproxy.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EventRecord {

    @JsonProperty("topic")
    private String topic;

    @JsonProperty("key")
    private String key;

    @JsonProperty("value")
    private Object value;

    @JsonProperty("partition")
    private int partition;

    @JsonProperty("offset")
    private long offset;

    public EventRecord() {
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String toString() {
        return "";
    }
}
