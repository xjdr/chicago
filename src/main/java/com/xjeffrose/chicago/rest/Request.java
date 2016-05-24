package com.xjeffrose.chicago.rest;

/**
 * Created by root on 5/24/16.
 */
public class Request {
    String key;
    String value;
    String columnFamily;


    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }


}
