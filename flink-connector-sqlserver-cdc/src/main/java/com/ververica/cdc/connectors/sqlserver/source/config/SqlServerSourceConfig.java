package com.ververica.cdc.connectors.sqlserver.source.config;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Created by yexianxun@corp.netease.com on 2021/11/30.
 */
public class SqlServerSourceConfig implements Serializable {
	private static final long serialVersionUID = 1L;

	private final String hostname;
	private final String port;
	private final String username;
	private final String password;
	private final List<String> databaseList;
	private final List<String> tableList;

	// --------------------------------------------------------------------------------------------
	// Debezium Configurations
	// --------------------------------------------------------------------------------------------
	private final Properties dbzProperties;
	private final Configuration dbzConfiguration;
	private final SqlServerConnectorConfig dbzSqlServerConfig;

	public SqlServerSourceConfig(String hostname, String port, String username, String password, List<String> databaseList, List<String> tableList,
								 Properties dbzProperties) {
		this.hostname = hostname;
		this.port = port;
		this.username = username;
		this.password = password;
		this.databaseList = databaseList;
		this.tableList = tableList;

		this.dbzProperties = checkNotNull(dbzProperties);
		this.dbzConfiguration = Configuration.from(dbzProperties);
		this.dbzSqlServerConfig = new SqlServerConnectorConfig(dbzConfiguration);
	}

	public static long getSerialVersionUID() {
		return serialVersionUID;
	}

	public String getHostname() {
		return hostname;
	}

	public String getPort() {
		return port;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public List<String> getDatabaseList() {
		return databaseList;
	}

	public List<String> getTableList() {
		return tableList;
	}

	public Properties getDbzProperties() {
		return dbzProperties;
	}

	public Configuration getDbzConfiguration() {
		return dbzConfiguration;
	}

	public SqlServerConnectorConfig getDbzSqlServerConfig() {
		return dbzSqlServerConfig;
	}
}
