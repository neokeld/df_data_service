package com.datafibers.flinknext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import com.datafibers.util.ConstantApp;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Deserialization schema from AVRO to {@link Row}.
 *
 * <p>Deserializes the <code>byte[]</code> messages as a AVROject and reads
 * the specified fields.
 *
 * <p>Failure during deserialization are forwarded as wrapped IOExceptions.
 */
public class AvroRowDeserializationSchema implements DeserializationSchema<Row> {
    private static final long serialVersionUID = 0x3C192A12BCAC82DBL;

    /** Field names to parse. Indices match fieldTypes indices. */
    private final String[] fieldNames;
    /** Types to parse fields as. Indices match fieldNames indices. */
    private final TypeInformation<?>[] fieldTypes;
    private final String staticAvroSchema;

    /** Object mapper for parsing the JSON. */
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** Flag indicating whether to fail on a missing field. */
    private boolean failOnMissingField;

    /** TODO - When schema changes, the Source table does not need to be recreated.*/

    /**
     * Creates a AVRO deserializtion schema for the given fields and type classes.
     *
     * @param fieldNames Names of AVRO fields to parse.
     * @param fieldTypes Type classes to parse JSON fields as.
     */
    public AvroRowDeserializationSchema(String[] fieldNames, Class<?>[] fieldTypes, Properties properties) {
        Preconditions.checkNotNull(properties, "properties");
        staticAvroSchema = properties.getProperty(ConstantApp.PK_SCHEMA_STR_INPUT);

        this.fieldNames = Preconditions.checkNotNull(fieldNames, "Field names");
        this.fieldTypes = new TypeInformation[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; ++i) {
			this.fieldTypes[i] = TypeExtractor.getForClass(fieldTypes[i]);
		}

        Preconditions.checkArgument(fieldNames.length == fieldTypes.length,
                "Number of provided field names and types does not match.");
    }

    /**
     * Creates a AVRO deserializtion schema for the given fields and types.
     *
     * @param fieldNames Names of AVRO fields to parse.
     * @param fieldTypes Types to parse AVRO fields as.
     */
    public AvroRowDeserializationSchema(String[] fieldNames, TypeInformation<?>[] fieldTypes, Properties properties) {
        Preconditions.checkNotNull(properties, "properties");
        staticAvroSchema = properties.getProperty(ConstantApp.PK_SCHEMA_STR_INPUT);

        this.fieldNames = Preconditions.checkNotNull(fieldNames, "Field names");
        this.fieldTypes = Preconditions.checkNotNull(fieldTypes, "Field types");

        Preconditions.checkArgument(fieldNames.length == fieldTypes.length,
                "Number of provided field names and types does not match.");
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        try {
            BinaryDecoder decoder;
            ByteBuffer buffer = ByteBuffer.wrap(message);

            if (buffer.get() != ConstantApp.MAGIC_BYTE) {
				decoder = DecoderFactory.get().binaryDecoder(message, null);
			} else {
                // For platform of confluent with SchemaRegister magic codec and dynamic schema
                buffer.getInt(); // Do not comment it out. Or else, set start as 5
                decoder = DecoderFactory.get().binaryDecoder(buffer.array(), buffer.position() + buffer.arrayOffset(),
						buffer.limit() - ConstantApp.idSize - 1, null);
            }
                        JsonNode root;
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(new Schema.Parser().parse(staticAvroSchema));//TODO get row level schema
            root = objectMapper.readTree(reader.read(null, decoder).toString());
            Row row = new Row(fieldNames.length);
            for (int i = 0; i < fieldNames.length; ++i) {
                JsonNode node = root.get(fieldNames[i]);

                if (node != null) {
					row.setField(i, objectMapper.treeToValue(node, fieldTypes[i].getTypeClass()));
				} else {
					if (failOnMissingField) {
						throw new IllegalStateException("Failed to find field with name '" + fieldNames[i] + "'.");
					}
					row.setField(i, null);
				}
            }

            return row;
        } catch (Exception t) {
            throw new IOException("Failed to deserialize AVRO object.", t);
        }
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    /**
     * Configures the failure behaviour if a JSON field is missing.
     *
     * <p>By default, a missing field is ignored and the field is set to null.
     *
     * @param failOnMissingField Flag indicating whether to fail or not on a missing field.
     */
    public void setFailOnMissingField(boolean failOnMissingField) {
        this.failOnMissingField = failOnMissingField;
    }
}
