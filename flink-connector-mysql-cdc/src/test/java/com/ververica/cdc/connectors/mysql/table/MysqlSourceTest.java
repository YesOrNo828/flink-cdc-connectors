package com.ververica.cdc.connectors.mysql.table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yexianxun@corp.netease.com on 2021/12/8.
 */
public class MysqlSourceTest {
	private static final Logger LOG = LoggerFactory.getLogger(MysqlSourceTest.class);

	@Test
	public void testSource() throws Exception {
		LOG.info("begin...");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3); // source only supports parallelism of 1
		env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);

		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		// register a table in the catalog
		tEnv.executeSql(
				"CREATE TABLE pk_uk_id (\n"
						+ "     id INT,\n"
						+ "     ukey1 int ,\n"
						+ "     ukey2 int ,\n"
						+ "     PRIMARY KEY(id) NOT ENFORCED\n"
						+ "     ) WITH (\n"
						+ "     'connector' = 'mysql-cdc',\n"
						+ "     'hostname' = 'localhost',\n"
						+ "     'port' = '3306',\n"
						+ "     'username' = 'root',\n"
						+ "     'password' = '123456',\n"
						+ "     'database-name' = 'testdb',\n"
						+ "     'table-name' = 'pk_uk_from')");

		final Table result = tEnv.sqlQuery("select * from pk_uk_id");
		tEnv.toRetractStream(result, RowData.class)
				.map((MapFunction<Tuple2<Boolean, RowData>, String>) value -> {
					RowData rowData = value.f1;

					return rowData.getRowKind() + "," + rowData.getInt(0) + "," + rowData.getInt(1) + "," + rowData.getInt(2);
				})
				.setParallelism(2)
				.print();

//        tEnv.toRetractStream(result, RowData.class)
//                .map(new MapFunction<Tuple2<Boolean, RowData>, RowData>() {
//                    @Override
//                    public RowData map(Tuple2<Boolean, RowData> value) throws Exception {
//                        return value.f1;
//                    }
//                })
//                .print();
        env.execute();

//		tEnv.executeSql(
//				"insert into print/*+ OPTIONS('print-identifier'='val') */ SELECT vi.name, Max(score) as score_max FROM  vi"
//						+ " JOIN dim for system_time as of vi.proctime as dim on vi.name=dim.name"
//						+ " GROUP BY vi.name");

	}
}
