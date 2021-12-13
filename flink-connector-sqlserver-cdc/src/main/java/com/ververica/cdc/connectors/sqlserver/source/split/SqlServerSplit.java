package com.ververica.cdc.connectors.sqlserver.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

/** The split of table comes from a Table that splits by primary key. */
public class SqlServerSplit implements SourceSplit {

	protected final String splitId;

	public SqlServerSplit(String splitId) {
		this.splitId = splitId;
	}

	@Override
	public String splitId() {
		return splitId;
	}
}
