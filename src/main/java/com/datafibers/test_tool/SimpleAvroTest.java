package com.datafibers.test_tool;

import static org.apache.avro.Schema.Type.RECORD;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.lang3.StringUtils;

import com.datafibers.util.SchemaRegistryClient;

public class SimpleAvroTest {
    public static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "  { \"name\":\"symbol\", \"type\":\"string\" },"
            + "  { \"name\":\"name\", \"type\":\"string\" },"
            + "  { \"name\":\"exchangecode\", \"type\":\"string\" }"
            + "]}";

    public static void main(String[] args) throws InterruptedException {
        Schema schema = new Schema.Parser().parse(USER_SCHEMA);
        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("symbol", "CHINA");
        user1.put("exchangecode", "TEST");

        try {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder( out, null );
            writer.write(user1, encoder );
            encoder.flush();

            System.out.println(new GenericDatumReader<GenericRecord>(schema).read(null,
					DecoderFactory.get().binaryDecoder(out.toByteArray(), null)).toString());

            System.out.println("******Get Fields Names");

            List<String> stringList = new ArrayList<>();
            if (RECORD.equals(schema.getType()) && schema.getFields() != null
                    && !schema.getFields().isEmpty()) {
				for (Schema.Field field : schema.getFields()) {
					stringList.add(field.name());
				}
			}
            String[] fieldNames = stringList.toArray( new String[] {} );
            for ( String element : fieldNames ) {
				System.out.println(element);
			}

            System.out.println("******Get Fields Types");

            Class<?>[] fieldTypes = new Class[schema.getFields().size()];
            int index = 0;
            String typeName;

            try {
                if (RECORD.equals(schema.getType()) && schema.getFields() != null
                        && !schema.getFields().isEmpty()) {
					for (Schema.Field field : schema.getFields()) {
						typeName = field.schema().getType().getName().toLowerCase();
						switch (typeName) {
						default:
							fieldTypes[index] = Class.forName("java.lang." + StringUtils.capitalize(typeName));
							break;
						case "bytes":
							fieldTypes[index] = Class.forName("java.util.Byte");
							break;
						case "int":
							fieldTypes[index] = Class.forName("java.lang.Integer");
							break;
						}
						++index;
					}
				}
            } catch (ClassNotFoundException cnf) {
                cnf.printStackTrace();
            }

            for ( Class<?> element : fieldTypes ) {
				System.out.println(element);
			}

            System.out.println("TestCase_Test Schema Register Client");

            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "localhost:9092");
            properties.setProperty("group.id", "consumer_test");
            properties.setProperty("schema.subject", "test-value");
            properties.setProperty("schema.registry", "localhost:8081");

            try {
                System.out.println("raw schema1 for name is " + SchemaRegistryClient.getLatestSchemaFromProperty(properties).getField("name"));

                String USER_SCHEMA = "{"
                        + "\"type\":\"record\","
                        + "\"name\":\"test\","
                        + "\"fields\":["
                        + "  { \"name\":\"name\", \"type\":\"string\" },"
                        + "  { \"name\":\"symbol\", \"type\":\"string\" },"
                        + "  { \"name\":\"exchangecode\", \"type\":\"string\" }"
                        + "]}";
                new Schema.Parser().parse(USER_SCHEMA);
				System.out.println("raw schema2 for name is " + schema.getField("name"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}