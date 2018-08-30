package com.datafibers.test_tool;

import com.datafibers.flinknext.Kafka010AvroTableSource;
import com.datafibers.flinknext.Kafka09AvroTableSink;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.SchemaRegistryClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.log4j.Logger;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

/** TC for Flink features. */
public class TCFlinkAvroSQL {
    private static final Logger LOG = Logger.getLogger(TCFlinkAvroSQL.class);

    public static void tcFlinkAvroSQL(String schemaRegistryHostPort, String srcTopic, String targetTopic, String sqlState) {
        LOG.info("tcFlinkAvroSQL");
        String resultFile = "testResult";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 6123, "C:/Users/dadu/Coding/df_data_service/target/df-data-service-1.1-SNAPSHOT-fat.jar")
                .setParallelism(1);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        Properties properties = new Properties();
        properties.setProperty(ConstantApp.PK_KAFKA_HOST_PORT.replace("_", "."), "localhost:9092");
        properties.setProperty(ConstantApp.PK_KAFKA_CONSUMER_GROURP, "consumer_test");
        properties.setProperty(ConstantApp.PK_KAFKA_SCHEMA_REGISTRY_HOST_PORT.replace("_", "."), schemaRegistryHostPort);
        properties.setProperty(ConstantApp.PK_FLINK_TABLE_SINK_KEYS, "symbol");

        String[] srcTopicList = srcTopic.split(",");
        for (int i = 0; i < srcTopicList.length; ++i) {
            properties.setProperty(ConstantApp.PK_SCHEMA_SUB_INPUT, srcTopicList[i]);
            properties.setProperty(ConstantApp.PK_SCHEMA_ID_INPUT, String.valueOf(SchemaRegistryClient.getLatestSchemaIDFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_INPUT)));
            properties.setProperty(ConstantApp.PK_SCHEMA_STR_INPUT, SchemaRegistryClient.getLatestSchemaFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_INPUT).toString());
            tableEnv.registerTableSource(srcTopicList[i], new Kafka010AvroTableSource(srcTopicList[i], properties));
        }

        try {
            Table result = tableEnv.sql(sqlState);
            result.printSchema();
            LOG.info("generated avro schema is = " + SchemaRegistryClient.tableAPIToAvroSchema(result, targetTopic));
            SchemaRegistryClient.addSchemaFromTableResult(schemaRegistryHostPort, targetTopic, result);

            // delivered properties
            properties.setProperty(ConstantApp.PK_SCHEMA_SUB_OUTPUT, targetTopic);
            properties.setProperty(ConstantApp.PK_SCHEMA_ID_OUTPUT, String.valueOf(SchemaRegistryClient.getLatestSchemaIDFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_OUTPUT)));
            properties.setProperty(ConstantApp.PK_SCHEMA_STR_OUTPUT, SchemaRegistryClient.getLatestSchemaFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_OUTPUT).toString());

            LOG.info(Paths.get(resultFile).toAbsolutePath());
            result.writeToSink(new Kafka09AvroTableSink(targetTopic, properties, new FlinkFixedPartitioner()));
            env.execute("tcFlinkAvroSQL");
        } catch (Exception e) {
        	LOG.error(Arrays.toString(e.getStackTrace()));
        }
    }

    public static void main(String[] args) {
        //TODO since sink avro only support primary type, we cannot select other unsupport type in the select statement

        tcFlinkAvroSQL("localhost:8002", "test_stock", "SQLSTATE_UNION_01",
				"SELECT symbol, bid_size FROM test_stock where symbol = 'FB' union all SELECT symbol, bid_size FROM test_stock where symbol = 'SAP'");
    }
}
