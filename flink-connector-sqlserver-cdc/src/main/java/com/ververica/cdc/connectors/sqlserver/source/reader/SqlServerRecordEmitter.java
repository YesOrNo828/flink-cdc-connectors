package com.ververica.cdc.connectors.sqlserver.source.reader;

import com.ververica.cdc.connectors.sqlserver.source.split.SqlServerSplitState;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yexianxun@corp.netease.com on 2021/11/30.
 */
public class SqlServerRecordEmitter<T>
		implements RecordEmitter<SourceRecord, T, SqlServerSplitState> {

	private static final Logger LOG = LoggerFactory.getLogger(SqlServerRecordEmitter.class);
	private final DebeziumDeserializationSchema<T> debeziumDeserializationSchema;
	private final OutputCollector<T> outputCollector;

	public SqlServerRecordEmitter(
			DebeziumDeserializationSchema<T> debeziumDeserializationSchema) {
		this.debeziumDeserializationSchema = debeziumDeserializationSchema;
		this.outputCollector = new OutputCollector<>();
	}

	@Override
	public void emitRecord(SourceRecord element, SourceOutput<T> output, SqlServerSplitState splitState) throws Exception {
		outputCollector.output = output;
		debeziumDeserializationSchema.deserialize(element, outputCollector);
	}

	private static class OutputCollector<T> implements Collector<T> {
		private SourceOutput<T> output;

		@Override
		public void collect(T record) {
			output.collect(record);
		}

		@Override
		public void close() {
			// do nothing
		}
	}
}
