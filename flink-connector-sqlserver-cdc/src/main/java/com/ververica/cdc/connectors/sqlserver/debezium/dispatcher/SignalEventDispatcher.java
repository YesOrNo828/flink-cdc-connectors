/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.sqlserver.debezium.dispatcher;

import io.debezium.util.SchemaNameAdjuster;

/**
 * A dispatcher to dispatch watermark signal events.
 *
 * <p>The watermark signal event is used to describe the start point and end point of a split scan.
 * The Watermark Signal Algorithm is inspired by https://arxiv.org/pdf/2010.12597v1.pdf.
 */
public class SignalEventDispatcher {

    private static final SchemaNameAdjuster SCHEMA_NAME_ADJUSTER = SchemaNameAdjuster.create();

    public static final String DATABASE_NAME = "db";
    public static final String TABLE_NAME = "table";
    public static final String WATERMARK_SIGNAL = "_split_watermark_signal_";
    public static final String SPLIT_ID_KEY = "split_id";
    public static final String BINLOG_FILENAME_OFFSET_KEY = "file";
    public static final String BINLOG_POSITION_OFFSET_KEY = "pos";
    public static final String WATERMARK_KIND = "watermark_kind";
    public static final String SIGNAL_EVENT_KEY_SCHEMA_NAME =
            "io.debezium.connector.flink.cdc.embedded.watermark.key";
    public static final String SIGNAL_EVENT_VALUE_SCHEMA_NAME =
            "io.debezium.connector.flink.cdc.embedded.watermark.value";

    /** The watermark kind. */
    public enum WatermarkKind {
        LOW,
        HIGH,
        BINLOG_END;

        public WatermarkKind fromString(String kindString) {
            if (LOW.name().equalsIgnoreCase(kindString)) {
                return LOW;
            } else if (HIGH.name().equalsIgnoreCase(kindString)) {
                return HIGH;
            } else {
                return BINLOG_END;
            }
        }
    }
}
