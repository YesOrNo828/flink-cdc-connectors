package com.ververica.cdc.connectors.sqlserver.source;

import com.ververica.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import com.ververica.cdc.connectors.sqlserver.source.enumerator.SqlServerSourceEnumState;
import com.ververica.cdc.connectors.sqlserver.source.reader.SqlServerRecordEmitter;
import com.ververica.cdc.connectors.sqlserver.source.reader.SqlServerSourceReader;
import com.ververica.cdc.connectors.sqlserver.source.reader.SqlServerSplitReader;
import com.ververica.cdc.connectors.sqlserver.source.split.SqlServerSplit;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.function.Supplier;

/**
 * Created by yexianxun@corp.netease.com on 2021/11/29.
 */
public class SqlServerSource<T> implements Source<T, SqlServerSplit, SqlServerSourceEnumState> {

	private final DebeziumDeserializationSchema<T> deserializationSchema;

	public SqlServerSource(DebeziumDeserializationSchema<T> deserializationSchema) {
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public Boundedness getBoundedness() {
		return Boundedness.CONTINUOUS_UNBOUNDED;
	}

	@Override
	public SourceReader<T, SqlServerSplit> createReader(SourceReaderContext readerContext) {

		SqlServerSourceConfig sourceConfig = new SqlServerSourceConfig(
				"", "", "", "", null, null, null
		);
		FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementsQueue =
				new FutureCompletingBlockingQueue<>();


		Supplier<SqlServerSplitReader> splitReaderSupplier =
				() -> new SqlServerSplitReader(sourceConfig, readerContext.getIndexOfSubtask());


		return new SqlServerSourceReader<>(
				elementsQueue,
				splitReaderSupplier,
				new SqlServerRecordEmitter<>(deserializationSchema),
				readerContext.getConfiguration(),
				readerContext

		);
	}

	@Override
	public SplitEnumerator<SqlServerSplit, SqlServerSourceEnumState> createEnumerator(
			SplitEnumeratorContext<SqlServerSplit> enumContext) throws Exception {
		return null;
	}

	@Override
	public SplitEnumerator<SqlServerSplit, SqlServerSourceEnumState> restoreEnumerator(SplitEnumeratorContext<SqlServerSplit> enumContext, SqlServerSourceEnumState checkpoint) throws Exception {
		return null;
	}

	@Override
	public SimpleVersionedSerializer<SqlServerSplit> getSplitSerializer() {
		return null;
	}

	@Override
	public SimpleVersionedSerializer<SqlServerSourceEnumState> getEnumeratorCheckpointSerializer() {
		return null;
	}

}
