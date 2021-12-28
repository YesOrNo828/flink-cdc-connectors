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

package com.ververica.cdc.connectors.refactor.refactor.source.config;

import com.ververica.cdc.connectors.base.source.config.SourceConfig;
import com.ververica.cdc.connectors.base.source.config.StartupOptions;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.RelationalTableFilters;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Describes the connection information of the Mysql database and the configuration information for
 * performing snapshotting and streaming reading, such as splitSize.
 */
public class MySqlSourceConfig extends SourceConfig {

    private final MySqlConnectorConfig dbzMySqlConfig;

    public MySqlSourceConfig(
            String driverClassName,
            String hostname,
            int port,
            String username,
            String password,
            List<String> databaseList,
            List<String> tableList,
            StartupOptions startupOptions,
            int splitSize,
            int splitMetaGroupSize,
            int fetchSize,
            String serverTimeZone,
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            boolean includeSchemaChanges,
            Properties dbzProperties,
            Configuration dbzConfiguration) {
        super(
                driverClassName,
                hostname,
                port,
                username,
                password,
                databaseList,
                tableList,
                startupOptions,
                splitSize,
                splitMetaGroupSize,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                dbzProperties,
                dbzConfiguration);
        this.dbzMySqlConfig = new MySqlConnectorConfig(dbzConfiguration);
    }

    public MySqlConnectorConfig getMySqlConnectorConfig() {
        return dbzMySqlConfig;
    }

    public RelationalTableFilters getTableFilters() {
        return dbzMySqlConfig.getTableFilters();
    }
}
