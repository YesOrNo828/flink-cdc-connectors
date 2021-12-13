///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.ververica.cdc.connectors.sqlserver.debezium.task.context;
//
//import com.github.shyiko.mysql.binlog.BinaryLogClient;
//import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
//import com.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory;
//import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
//import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
//import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
//import com.ververica.cdc.connectors.sqlserver.debezium.dispatcher.EventDispatcherImpl;
//import com.ververica.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
//import com.ververica.cdc.connectors.sqlserver.source.split.SqlServerSplit;
//import io.debezium.connector.AbstractSourceInfo;
//import io.debezium.connector.base.ChangeEventQueue;
//import io.debezium.connector.mysql.MySqlChangeEventSourceMetricsFactory;
//import io.debezium.connector.mysql.MySqlConnection;
//import io.debezium.connector.mysql.MySqlConnectorConfig;
//import io.debezium.connector.mysql.MySqlDatabaseSchema;
//import io.debezium.connector.mysql.MySqlErrorHandler;
//import io.debezium.connector.mysql.MySqlOffsetContext;
//import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
//import io.debezium.connector.mysql.MySqlTopicSelector;
//import io.debezium.connector.sqlserver.SqlServerConnection;
//import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
//import io.debezium.connector.sqlserver.SqlServerDatabaseSchema;
//import io.debezium.connector.sqlserver.SqlServerOffsetContext;
//import io.debezium.connector.sqlserver.SqlServerTopicSelector;
//import io.debezium.connector.sqlserver.SqlServerValueConverters;
//import io.debezium.data.Envelope;
//import io.debezium.pipeline.DataChangeEvent;
//import io.debezium.pipeline.ErrorHandler;
//import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
//import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
//import io.debezium.pipeline.source.spi.EventMetadataProvider;
//import io.debezium.pipeline.spi.OffsetContext;
//import io.debezium.relational.TableId;
//import io.debezium.schema.DataCollectionId;
//import io.debezium.schema.TopicSelector;
//import io.debezium.util.Clock;
//import io.debezium.util.Collect;
//import io.debezium.util.SchemaNameAdjuster;
//import org.apache.kafka.connect.data.Struct;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.time.Instant;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import static com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset.BINLOG_FILENAME_OFFSET_KEY;
//
///**
// * A stateful task context that contains entries the debezium mysql connector task required.
// *
// * <p>The offset change and schema change should record to MySqlSplitState when emit the record,
// * thus the Flink's state mechanism can help to store/restore when failover happens.
// */
//public class StatefulTaskContext {
//
//    private static final Logger LOG = LoggerFactory.getLogger(StatefulTaskContext.class);
//    private static final Clock clock = Clock.SYSTEM;
//
//    private final SqlServerSourceConfig sourceConfig;
//    private final SqlServerConnectorConfig connectorConfig;
//    private final SqlServerEventMetadataProvider metadataProvider;
//    private final SchemaNameAdjuster schemaNameAdjuster;
//    private final SqlServerConnection connection;
//
//    private SqlServerDatabaseSchema databaseSchema;
//    private SqlServerTaskContextImpl taskContext;
//    private SqlServerOffsetContext offsetContext;
//    private TopicSelector<TableId> topicSelector;
//    private SnapshotChangeEventSourceMetrics snapshotChangeEventSourceMetrics;
//    private StreamingChangeEventSourceMetrics streamingChangeEventSourceMetrics;
//    private EventDispatcherImpl<TableId> dispatcher;
//    private ChangeEventQueue<DataChangeEvent> queue;
//    private ErrorHandler errorHandler;
//
//    public StatefulTaskContext(
//            SqlServerSourceConfig sourceConfig,
//            SqlServerConnection connection) {
//        this.sourceConfig = sourceConfig;
//        this.connectorConfig = sourceConfig.getDbzSqlServerConfig();
//        this.schemaNameAdjuster = SchemaNameAdjuster.create();
//        this.metadataProvider = new SqlServerEventMetadataProvider();
//        this.connection = connection;
//    }
//
//    public void configure(SqlServerSplit sqlServerSplit) {
//        // initial stateful objects
//        this.topicSelector = SqlServerTopicSelector.defaultSelector(connectorConfig);
////        EmbeddedFlinkDatabaseHistory.registerHistory(
////                sourceConfig
////                        .getDbzConfiguration()
////                        .getString(EmbeddedFlinkDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME),
////                sqlServerSplit.getTableSchemas().values());
//
//        final SqlServerValueConverters valueConverters = new SqlServerValueConverters(connectorConfig.getDecimalMode(),
//                connectorConfig.getTemporalPrecisionMode(), connectorConfig.binaryHandlingMode());
//        final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
//        this.databaseSchema =
//                new SqlServerDatabaseSchema(
//                        connectorConfig,
//                        valueConverters,
//                        topicSelector,
//                        schemaNameAdjuster);
//        this.offsetContext =
//                loadStartingOffsetState(new SqlServerOffsetContext.Loader(connectorConfig), sqlServerSplit);
//        validateAndLoadDatabaseHistory(offsetContext, databaseSchema);
//
//        this.taskContext =
//                new SqlServerTaskContextImpl(connectorConfig, databaseSchema);
//        final int queueSize =
//                sqlServerSplit.isSnapshotSplit()
//                        ? Integer.MAX_VALUE
//                        : connectorConfig.getMaxQueueSize();
//        this.queue =
//                new ChangeEventQueue.Builder<DataChangeEvent>()
//                        .pollInterval(connectorConfig.getPollInterval())
//                        .maxBatchSize(connectorConfig.getMaxBatchSize())
//                        .maxQueueSize(queueSize)
//                        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
//                        .loggingContextSupplier(
//                                () ->
//                                        taskContext.configureLoggingContext(
//                                                "mysql-cdc-connector-task"))
//                        // do not buffer any element, we use signal event
//                        // .buffering()
//                        .build();
//        this.dispatcher =
//                new EventDispatcherImpl<>(
//                        connectorConfig,
//                        topicSelector,
//                        databaseSchema,
//                        queue,
//                        connectorConfig.getTableFilters().dataCollectionFilter(),
//                        DataChangeEvent::new,
//                        metadataProvider,
//                        schemaNameAdjuster);
//
//        final MySqlChangeEventSourceMetricsFactory changeEventSourceMetricsFactory =
//                new MySqlChangeEventSourceMetricsFactory(
//                        new MySqlStreamingChangeEventSourceMetrics(
//                                taskContext, queue, metadataProvider));
//        this.snapshotChangeEventSourceMetrics =
//                changeEventSourceMetricsFactory.getSnapshotMetrics(
//                        taskContext, queue, metadataProvider);
//        this.streamingChangeEventSourceMetrics =
//                changeEventSourceMetricsFactory.getStreamingMetrics(
//                        taskContext, queue, metadataProvider);
//        this.errorHandler = new MySqlErrorHandler(connectorConfig.getLogicalName(), queue);
//    }
//
//    private void validateAndLoadDatabaseHistory(
//            SqlServerOffsetContext offset, SqlServerDatabaseSchema schema) {
//        schema.initializeStorage();
//        schema.recover(offset);
//    }
//
//    /** Loads the connector's persistent offset (if present) via the given loader. */
//    private SqlServerOffsetContext loadStartingOffsetState(
//            OffsetContext.Loader loader, SqlServerSplit sqlServerSplit) {
//        Map<String, String> offset = new HashMap<>();
//
//
//        SqlServerOffsetContext sqlServerOffsetContext =
//                (SqlServerOffsetContext) loader.load(offset);
//
//        if (!isBinlogAvailable(sqlServerOffsetContext)) {
//            throw new IllegalStateException(
//                    "The connector is trying to read binlog starting at "
//                            + sqlServerOffsetContext.getSourceInfo()
//                            + ", but this is no longer "
//                            + "available on the server. Reconfigure the connector to use a snapshot when needed.");
//        }
//        return sqlServerOffsetContext;
//    }
//
//    private boolean isBinlogAvailable(SqlServerOffsetContext offset) {
//        String binlogFilename = offset.getSourceInfo().getString(BINLOG_FILENAME_OFFSET_KEY);
//        if (binlogFilename == null) {
//            return true; // start at current position
//        }
//        if (binlogFilename.equals("")) {
//            return true; // start at beginning
//        }
//
//        // Accumulate the available binlog filenames ...
//        List<String> logNames = connection.availableBinlogFiles();
//
//        // And compare with the one we're supposed to use ...
//        boolean found = logNames.stream().anyMatch(binlogFilename::equals);
//        if (!found) {
//            LOG.info(
//                    "Connector requires binlog file '{}', but MySQL only has {}",
//                    binlogFilename,
//                    String.join(", ", logNames));
//        } else {
//            LOG.info("MySQL has the binlog file '{}' required by the connector", binlogFilename);
//        }
//        return found;
//    }
//
//    /** Copied from debezium for accessing here. */
//    public static class SqlServerEventMetadataProvider implements EventMetadataProvider {
//        public static final String SERVER_ID_KEY = "server_id";
//
//        public static final String GTID_KEY = "gtid";
//        public static final String BINLOG_FILENAME_OFFSET_KEY = "file";
//        public static final String BINLOG_POSITION_OFFSET_KEY = "pos";
//        public static final String BINLOG_ROW_IN_EVENT_OFFSET_KEY = "row";
//        public static final String THREAD_KEY = "thread";
//        public static final String QUERY_KEY = "query";
//
//        @Override
//        public Instant getEventTimestamp(
//                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
//            if (value == null) {
//                return null;
//            }
//            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
//            if (source == null) {
//                return null;
//            }
//            final Long timestamp = sourceInfo.getInt64(AbstractSourceInfo.TIMESTAMP_KEY);
//            return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
//        }
//
//        @Override
//        public Map<String, String> getEventSourcePosition(
//                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
//            if (value == null) {
//                return null;
//            }
//            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
//            if (source == null) {
//                return null;
//            }
//            return Collect.hashMapOf(
//                    BINLOG_FILENAME_OFFSET_KEY,
//                    sourceInfo.getString(BINLOG_FILENAME_OFFSET_KEY),
//                    BINLOG_POSITION_OFFSET_KEY,
//                    Long.toString(sourceInfo.getInt64(BINLOG_POSITION_OFFSET_KEY)),
//                    BINLOG_ROW_IN_EVENT_OFFSET_KEY,
//                    Integer.toString(sourceInfo.getInt32(BINLOG_ROW_IN_EVENT_OFFSET_KEY)));
//        }
//
//        @Override
//        public String getTransactionId(
//                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
//            return ((MySqlOffsetContext) offset).getTransactionId();
//        }
//    }
//
//    public static Clock getClock() {
//        return clock;
//    }
//
//    public MySqlSourceConfig getSourceConfig() {
//        return sourceConfig;
//    }
//
//    public MySqlConnectorConfig getConnectorConfig() {
//        return connectorConfig;
//    }
//
//    public MySqlConnection getConnection() {
//        return connection;
//    }
//
//    public BinaryLogClient getBinaryLogClient() {
//        return binaryLogClient;
//    }
//
//    public MySqlDatabaseSchema getDatabaseSchema() {
//        return databaseSchema;
//    }
//
//    public SqlServerTaskContextImpl getTaskContext() {
//        return taskContext;
//    }
//
//    public EventDispatcherImpl<TableId> getDispatcher() {
//        return dispatcher;
//    }
//
//    public ChangeEventQueue<DataChangeEvent> getQueue() {
//        return queue;
//    }
//
//    public ErrorHandler getErrorHandler() {
//        return errorHandler;
//    }
//
//    public MySqlOffsetContext getOffsetContext() {
//        return offsetContext;
//    }
//
//    public TopicSelector<TableId> getTopicSelector() {
//        return topicSelector;
//    }
//
//    public SnapshotChangeEventSourceMetrics getSnapshotChangeEventSourceMetrics() {
//        snapshotChangeEventSourceMetrics.reset();
//        return snapshotChangeEventSourceMetrics;
//    }
//
//    public StreamingChangeEventSourceMetrics getStreamingChangeEventSourceMetrics() {
//        streamingChangeEventSourceMetrics.reset();
//        return streamingChangeEventSourceMetrics;
//    }
//
//    public SchemaNameAdjuster getSchemaNameAdjuster() {
//        return schemaNameAdjuster;
//    }
//}
