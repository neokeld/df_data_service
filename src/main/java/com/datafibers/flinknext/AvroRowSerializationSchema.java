package com.datafibers.flinknext;

import com.datafibers.util.ConstantApp;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.types.Row;
import org.apache.log4j.Logger;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;


/**
 * Deserialization schema from AVRO to {@link Row}.
 *
 * <p>Deserializes the <code>byte[]</code> messages as a AVROject and reads
 * the specified fields.
 *
 * <p>Failure during deserialization are forwarded as wrapped IOExceptions.
 */
public class AvroRowSerializationSchema implements SerializationSchema<Tuple2<Boolean, Row>> {

    private static final long serialVersionUID = 0x3C192A12BCAC82DCL;
    private static final Logger LOG = Logger.getLogger(AvroRowSerializationSchema.class);


	/**
	 * Low-level class for serialization of Avro values.
	 */

    protected final Properties properties;
    

    /** Generic Avro Schema reader for the row */
    private transient DatumWriter<Object> writer;

    /** TODO - When schema changes, the Source table does not need to be recreated.*/

    /**
     * Creates a AVRO serializtion schema for the given schema.
     *
     * @param schema Names of AVRO fields to parse.
     */


    public AvroRowSerializationSchema(Properties properties) {
        this.properties = properties;
    }
    @Override
    public byte[] serialize(Tuple2<Boolean, Row> b) {
        try {
            int schemaId = Integer.parseInt(properties.get(ConstantApp.PK_SCHEMA_ID_OUTPUT).toString());

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(ConstantApp.MAGIC_BYTE);
            out.write(ByteBuffer.allocate(ConstantApp.idSize).putInt(schemaId).array());

            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
            Schema schema = new Schema.Parser().parse(properties.get(ConstantApp.PK_SCHEMA_STR_OUTPUT).toString());

            new GenericDatumWriter<Object>(schema).write(convertToRecord(schema, b.f1), encoder); //TODO TO CHECK
            encoder.flush();

            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;

        } catch (IOException t) {
            t.printStackTrace();

        	throw new RuntimeException("Failed to serialize Row.", t);
        }


    }

    /**
     * Converts a (nested) Flink Row into Avro's {@link GenericRecord}.
     * Strings are converted into Avro's {@link Utf8} fields.
     */
    private static Object convertToRecord(Schema s, Object rowObj) {
		if (!(rowObj instanceof Row))
			return !(rowObj instanceof String) ? rowObj : new Utf8((String) rowObj);
		if (s.getType() != Schema.Type.UNION) {
			if (s.getType() != Schema.Type.RECORD)
				throw new RuntimeException("Record type for row type expected. But is: " + s);
		} else {
			final List<Schema> types = s.getTypes();
			if (types.size() == 2 && types.get(0).getType() == Schema.Type.NULL
					&& types.get(1).getType() == Schema.Type.RECORD)
				s = types.get(1);
			else {
				if (types.size() != 2 || types.get(0).getType() != Schema.Type.RECORD
						|| types.get(1).getType() != Schema.Type.NULL)
					throw new RuntimeException(
							"Currently we only support schemas of the following form: UNION[null, RECORD] or UNION[RECORD, NULL] Given: "
									+ s);
				s = types.get(0);
			}
		}
		final List<Schema.Field> fields = s.getFields();
		final GenericRecord record = new GenericData.Record(s);
		final Row row = (Row) rowObj;
		for (int i = 0; i < fields.size(); ++i) {
			final Schema.Field field = fields.get(i);
			record.put(field.pos(), convertToRecord(field.schema(), row.getField(i)));
		}
		return record;
	}

}
