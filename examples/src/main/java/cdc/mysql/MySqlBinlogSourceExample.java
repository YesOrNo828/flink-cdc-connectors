package cdc.mysql;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySqlBinlogSourceExample {
    public static void main(String[] args) throws Exception {
//        RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
//        TypeInformation<RowData> typeInfo = Types.ROW();
//        ZoneId serverTimeZone = ZoneId.of("UTC");
//        DebeziumDeserializationSchema<RowData> deserializer = new RowDataDebeziumDeserializeSchema(
//                rowType,
//                typeInfo,
//                ((rowData, rowKind) -> {}),
//                serverTimeZone);
		MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.122.173.167")
                .port(3306)
                .databaseList("flink-test") // monitor all tables under inventory database
                .username("sys")
                .password("netease")
                .tableList("flink-test.user_info")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute();
    }
}
