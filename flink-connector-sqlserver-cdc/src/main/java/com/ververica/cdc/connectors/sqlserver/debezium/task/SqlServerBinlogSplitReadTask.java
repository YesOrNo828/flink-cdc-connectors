package com.ververica.cdc.connectors.sqlserver.debezium.task;

import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerDatabaseSchema;
import io.debezium.connector.sqlserver.SqlServerOffsetContext;
import io.debezium.connector.sqlserver.SqlServerStreamingChangeEventSource;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * Created by yexianxun@corp.netease.com on 2021/11/30.
 */
public class SqlServerBinlogSplitReadTask extends SqlServerStreamingChangeEventSource {


	public SqlServerBinlogSplitReadTask(
			SqlServerConnectorConfig connectorConfig,
			SqlServerOffsetContext offsetContext,
			SqlServerConnection dataConnection,
			SqlServerConnection metadataConnection,
			EventDispatcher<TableId> dispatcher,
			ErrorHandler errorHandler,
			Clock clock,
			SqlServerDatabaseSchema schema) {
		super(
				connectorConfig,
				offsetContext,
				dataConnection,
				metadataConnection,
				dispatcher,
				errorHandler,
				clock,
				schema);

	}


}
