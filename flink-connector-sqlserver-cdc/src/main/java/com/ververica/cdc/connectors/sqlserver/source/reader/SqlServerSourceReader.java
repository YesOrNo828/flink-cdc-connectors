package com.ververica.cdc.connectors.sqlserver.source.reader;

import com.ververica.cdc.connectors.sqlserver.source.split.SqlServerSplit;
import com.ververica.cdc.connectors.sqlserver.source.split.SqlServerSplitState;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Created by yexianxun@corp.netease.com on 2021/11/29.
 */
public class SqlServerSourceReader<T>
		extends SingleThreadMultiplexSourceReaderBase<
		SourceRecord, T, SqlServerSplit, SqlServerSplitState> {

	public SqlServerSourceReader(
			FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementQueue,
			Supplier<SqlServerSplitReader> splitReaderSupplier,
			RecordEmitter<SourceRecord, T, SqlServerSplitState> recordEmitter,
			Configuration config,
			SourceReaderContext context) {
		super(
				elementQueue,
				new SingleThreadFetcherManager<>(elementQueue, splitReaderSupplier::get),
				recordEmitter,
				config,
				context);
	}

	@Override
	protected void onSplitFinished(Map<String, SqlServerSplitState> finishedSplitIds) {

	}

	@Override
	protected SqlServerSplitState initializedState(SqlServerSplit split) {
		return null;
	}

	@Override
	protected SqlServerSplit toSplitType(String splitId, SqlServerSplitState splitState) {
		return null;
	}
}
