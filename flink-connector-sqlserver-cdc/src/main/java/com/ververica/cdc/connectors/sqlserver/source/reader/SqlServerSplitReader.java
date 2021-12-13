package com.ververica.cdc.connectors.sqlserver.source.reader;

import com.ververica.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import com.ververica.cdc.connectors.sqlserver.source.split.SqlServerSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Created by yexianxun@corp.netease.com on 2021/11/29.
 */
public class SqlServerSplitReader implements SplitReader<SourceRecord, SqlServerSplit> {

	private static final Logger LOG = LoggerFactory.getLogger(SqlServerSplitReader.class);
	private final Queue<SqlServerSplit> splits;
	private final SqlServerSourceConfig sourceConfig;
	private final int subtaskId;

	@Nullable
	private String currentSplitId;

	public SqlServerSplitReader(SqlServerSourceConfig sourceConfig, int subtaskId) {
		this.sourceConfig = sourceConfig;
		this.subtaskId = subtaskId;
		this.splits = new ArrayDeque<>();
	}

	@Override
	public RecordsWithSplitIds<SourceRecord> fetch() throws IOException {
		return null;
	}

	@Override
	public void handleSplitsChanges(SplitsChange<SqlServerSplit> splitsChanges) {
		if (!(splitsChanges instanceof SplitsAddition)) {
			throw new UnsupportedOperationException(
					String.format(
							"The SplitChange type of %s is not supported.",
							splitsChanges.getClass()));
		}

		LOG.debug("Handling split change {}", splitsChanges);
		splits.addAll(splitsChanges.splits());
	}

	@Override
	public void wakeUp() {

	}

	@Override
	public void close() throws Exception {

	}
}
