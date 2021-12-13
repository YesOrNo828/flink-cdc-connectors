package com.ververica.cdc.connectors.sqlserver.source.enumerator;

import com.ververica.cdc.connectors.sqlserver.source.split.SqlServerSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

/**
 * Created by yexianxun@corp.netease.com on 2021/11/29.
 */
public class SqlServerSourceEnumerator implements SplitEnumerator<SqlServerSplit, SqlServerSourceEnumState> {
	@Override
	public void start() {

	}

	@Override
	public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

	}

	@Override
	public void addSplitsBack(List<SqlServerSplit> splits, int subtaskId) {

	}

	@Override
	public void addReader(int subtaskId) {

	}

	@Override
	public SqlServerSourceEnumState snapshotState(long checkpointId) throws Exception {
		return null;
	}

	@Override
	public void close() throws IOException {

	}
}
