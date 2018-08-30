package com.datafibers.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.streaming.api.datastream.DataStream;

/** Interface for dynamic Flink stable api script generation and running. */
public interface DynamicRunner {
    /** Run Flink Table API Transformation. */
    default void runTransform(DataSet<String> s) {
    }

    default void runTransform(DataStream<String> s) {
    }

    default Table transTableObj(Table tbl) {
        return tbl;
    }
}
