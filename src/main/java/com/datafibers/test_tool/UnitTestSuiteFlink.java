package com.datafibers.test_tool;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import net.openhft.compiler.CompilerUtils;

import org.apache.avro.Schema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.log4j.Logger;
import com.datafibers.flinknext.DFRemoteStreamEnvironment;
import com.datafibers.flinknext.Kafka09AvroTableSink;
import com.datafibers.flinknext.Kafka09AvroTableSource;
import com.datafibers.model.DFJobPOPJ;
import com.datafibers.service.DFInitService;
import com.datafibers.util.DynamicRunner;
import com.datafibers.util.SchemaRegistryClient;

/** TC for Flink features. */
public class UnitTestSuiteFlink {
    private static final String SELECT_NAME_SYMBOL_EXCHANGECODE_FROM_ORDERS = "SELECT name, symbol, exchangecode FROM Orders";
	private static final String ORDERS = "Orders";
	private static final String FLINK_AVRO_SQL_KAFKA_TEST = "Flink AVRO SQL KAFKA Test";
	private static final String HOME_VAGRANT_TEST_TXT = "/home/vagrant/test.txt";
	private static final Logger LOG = Logger.getLogger(UnitTestSuiteFlink.class);

    public static void testFlinkSQL() {
        LOG.info("Only Unit Testing Function is enabled");

        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 6123, DFInitService.class.getProtectionDomain().getCodeSource().getLocation().getPath())
                    .setParallelism(1);
            final String kafkaTopic = "finance", kafkaTopicStage = "df_trans_stage_finance";
            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "localhost:9092");
            properties.setProperty("group.id", "consumer3");

            // Internal covert Json String to Json - Begin
            DataStream<String> stream = env
                    .addSource(new FlinkKafkaConsumer09<>(kafkaTopic, new SimpleStringSchema(), properties));

            stream.map(new MapFunction<String, String>() {
                private static final long serialVersionUID = -0x7B2E50C728FE4502L;

				@Override
                public String map(String jsonString) throws Exception {
                    return jsonString.replaceAll("\\\\", "").replace("\"{", "{").replace("}\"","}");
                }
            }).addSink(new FlinkKafkaProducer09<String>("localhost:9092", kafkaTopicStage, new SimpleStringSchema()));
            // Internal covert Json String to Json - End

            String[] fieldNames =  new String[] {"name"};
            Class<?>[] fieldTypes = new Class<?>[] {String.class};

            tableEnv.registerTableSource(ORDERS, new Kafka09AvroTableSource(kafkaTopicStage, properties, fieldNames, fieldTypes));

            Table result = tableEnv.sql("SELECT name FROM Orders");

            Files.deleteIfExists(Paths.get(HOME_VAGRANT_TEST_TXT));

            // write the result Table to the TableSink
            result.writeToSink(new CsvTableSink(HOME_VAGRANT_TEST_TXT, "|"));

            env.execute("FlinkConsumer");
        } catch (Exception e) {
            LOG.error(Arrays.toString(e.getStackTrace()));
        }
    }

    public static void testFlinkAvroSQL() {
    	LOG.info("TestCase_Test Avro SQL");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 6123, DFInitService.class.getProtectionDomain().getCodeSource().getLocation().getPath())
                .setParallelism(1);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer_test");
        properties.setProperty("schema.subject", "test-value");
        properties.setProperty("schema.registry", "localhost:8081");
        properties.setProperty("static.avro.schema", "empty_schema");

        try {
            tableEnv.registerTableSource(ORDERS, new Kafka09AvroTableSource("test", properties));

            Table result = tableEnv.sql(SELECT_NAME_SYMBOL_EXCHANGECODE_FROM_ORDERS);

            Files.deleteIfExists(Paths.get(HOME_VAGRANT_TEST_TXT));

            // write the result Table to the TableSink
            result.writeToSink(new CsvTableSink(HOME_VAGRANT_TEST_TXT, "|"));
            env.execute(FLINK_AVRO_SQL_KAFKA_TEST);
        } catch (Exception e) {
        	LOG.error(Arrays.toString(e.getStackTrace()));
        }
    }

    public static void testFlinkAvroSQLWithStaticSchema() {
    	LOG.info("TestCase_Test Avro SQL with static Schema");

        final String STATIC_USER_SCHEMA = "{"
                + "\"type\":\"record\","
                + "\"name\":\"myrecord\","
                + "\"fields\":["
                + "  { \"name\":\"symbol\", \"type\":\"string\" },"
                + "  { \"name\":\"name\", \"type\":\"string\" },"
                + "  { \"name\":\"exchangecode\", \"type\":\"string\" }"
                + "]}";
        String resultFile = HOME_VAGRANT_TEST_TXT;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 6123, DFInitService.class.getProtectionDomain().getCodeSource().getLocation().getPath())
                .setParallelism(1);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer_test");
        properties.setProperty("schema.subject", "test-value");
        properties.setProperty("schema.registry", "localhost:8081");
        properties.setProperty("static.avro.schema", STATIC_USER_SCHEMA);

        try {
            tableEnv.registerTableSource(ORDERS, new Kafka09AvroTableSource("test", properties));

            Table result = tableEnv.sql(SELECT_NAME_SYMBOL_EXCHANGECODE_FROM_ORDERS);

            Files.deleteIfExists(Paths.get(resultFile));

            // write the result Table to the TableSink
            result.writeToSink(new CsvTableSink(resultFile, "|"));
            env.execute(FLINK_AVRO_SQL_KAFKA_TEST);
        } catch (Exception e) {
        	LOG.error(Arrays.toString(e.getStackTrace()));
        }
    }

    public static void testFlinkAvroSQLJson() {
    	LOG.info("TestCase_Test Avro SQL to Json Sink");
        String jarPath = DFInitService.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        DFRemoteStreamEnvironment env = new DFRemoteStreamEnvironment("localhost", 6123, jarPath)
                .setParallelism(1);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer_test");
        properties.setProperty("schema.subject", "test-value");
        properties.setProperty("schema.registry", "localhost:8081");
        properties.setProperty("useAvro", "avro");
        properties.setProperty("static.avro.schema",
                SchemaRegistryClient.getSchemaFromRegistry("http://localhost:8081", "test-value", "latest").toString());

        try {
            HashMap<String, String> hm = new HashMap<>();
            tableEnv.registerTableSource(ORDERS, new Kafka09AvroTableSource("test", properties));

            Table result = tableEnv.sql(SELECT_NAME_SYMBOL_EXCHANGECODE_FROM_ORDERS);
            // write the result Table to the TableSink
            result.writeToSink(new Kafka09AvroTableSink("test_json", properties, new FlinkFixedPartitioner()));
            env.executeWithDFObj(FLINK_AVRO_SQL_KAFKA_TEST, new DFJobPOPJ().setJobConfig(hm) );
        } catch (Exception e) {
        	LOG.error(Arrays.toString(e.getStackTrace()));
        }
    }

    public static void testSchemaRegisterClient() {
    	LOG.info("TestCase_Test Schema Register Client");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer_test");
        properties.setProperty("schema.subject", "test-value");
        properties.setProperty("schema.registry", "localhost:8081");

        try {
            Schema schema = SchemaRegistryClient.getLatestSchemaFromProperty(properties);
            LOG.info("raw schema1 for name is " + schema.getField("name"));

            final String USER_SCHEMA = "{"
                    + "\"type\":\"record\","
                    + "\"name\":\"test\","
                    + "\"fields\":["
                    + "  { \"name\":\"name\", \"type\":\"string\" },"
                    + "  { \"name\":\"symbol\", \"type\":\"string\" },"
                    + "  { \"name\":\"exchangecode\", \"type\":\"string\" }"
                    + "]}";
            new Schema.Parser().parse(USER_SCHEMA);
            LOG.info("raw schema2 for name is " + schema.getField("name"));
        } catch (Exception e) {
        	LOG.error(Arrays.toString(e.getStackTrace()));
        }
    }

    public static void testFlinkAvroScriptWithStaticSchema() {
    	LOG.info("TestCase_Test Avro Table API Script with static Schema");

        final String STATIC_USER_SCHEMA = "{"
                + "\"type\":\"record\","
                + "\"name\":\"myrecord\","
                + "\"fields\":["
                + "  { \"name\":\"symbol\", \"type\":\"string\" },"
                + "  { \"name\":\"name\", \"type\":\"string\" },"
                + "  { \"name\":\"exchangecode\", \"type\":\"string\" }"
                + "]}";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 6123, DFInitService.class.getProtectionDomain().getCodeSource().getLocation().getPath())
                .setParallelism(1);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer_test");
        properties.setProperty("schema.subject", "test-value");
        properties.setProperty("schema.registry", "localhost:8081");
        properties.setProperty("static.avro.schema", STATIC_USER_SCHEMA);

        try {
            tableEnv.registerTableSource(ORDERS, new Kafka09AvroTableSource("test", properties));
            final String javaCode = "package dynamic;\n" +
                    "import org.apache.flink.table.api.Table;\n" +
                    "import com.datafibers.util.*;\n" +
                    "public class FlinkScript implements DynamicRunner {\n" +
                    "@Override \n" +
                    "    public Table transTableObj(Table tbl) {\n" +
                    "try {" +
                    "return tbl."+ "select(\"name\")" + ";" +
                    "} catch (Exception e) {" +
                    "};" +
                    "return null;}}";
            // Dynamic code generation
            Table result = ((DynamicRunner) CompilerUtils.CACHED_COMPILER.loadFromJava("dynamic.FlinkScript", javaCode).newInstance()).transTableObj(tableEnv.scan(ORDERS));
            // write the result Table to the TableSink
            result.writeToSink(new Kafka09AvroTableSink ("test_json", properties, new FlinkFixedPartitioner()));
            env.execute(FLINK_AVRO_SQL_KAFKA_TEST);
        } catch (Exception e) {
        	LOG.error(Arrays.toString(e.getStackTrace()));
        }
    }

    public static void main(String[] args) {
    	LOG.info(SchemaRegistryClient.getSchemaFromRegistry ("http://localhost:8081", "test-value", "latest"));
        testFlinkAvroSQLJson();
    }
}
