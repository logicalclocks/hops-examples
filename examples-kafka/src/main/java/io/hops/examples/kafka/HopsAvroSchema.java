package io.hops.examples.kafka;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import io.hops.kafkautil.HopsKafkaUtil;
import io.hops.kafkautil.SchemaNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 *
 * <p>
 */
public class HopsAvroSchema implements DeserializationSchema<String>,
        SerializationSchema<Tuple4<String, String, String, String>> {

  private static final long serialVersionUID = 1L;
  private String schemaJson;
  private transient Schema.Parser parser = new Schema.Parser();
  private transient Schema schema;
  private transient Injection<GenericRecord, byte[]> recordInjection
          = GenericAvroCodecs.
          toBinary(schema);
  private boolean initialized = false;

  public HopsAvroSchema(String topicName) {
    try {
      schemaJson = HopsKafkaUtil.getInstance().getSchema(topicName);
    } catch (SchemaNotFoundException ex) {
      Logger.getLogger(HopsAvroSchema.class.getName()).log(Level.SEVERE, null,
              ex);
    }
  }

  @Override
  public String deserialize(byte[] bytes) throws IOException {
    if (!initialized) {
      parser = new Schema.Parser();
      schema = parser.parse(schemaJson);
      recordInjection = GenericAvroCodecs.toBinary(schema);
      initialized = true;
    }
    GenericRecord genericRecord = recordInjection.invert(bytes).get();
    return genericRecord.toString().replaceAll("\\\\u001A", "");
  }

  @Override
  public boolean isEndOfStream(String t) {
    return false;
  }

  @Override
  public TypeInformation<String> getProducedType() {
    return BasicTypeInfo.STRING_TYPE_INFO;
  }

  @Override
  public byte[] serialize(Tuple4<String, String, String, String> t) {

    if (!initialized) {
      parser = new Schema.Parser();
      schema = parser.parse(schemaJson);
      recordInjection = GenericAvroCodecs.toBinary(schema);
      initialized = true;
    }
    GenericData.Record avroRecord = new GenericData.Record(schema);
    for (int i = 0; i < t.getArity() - 1; i += 2) {
      avroRecord.put(t.getField(i).toString(), t.getField(i + 1).toString());
    }

    byte[] bytes = recordInjection.apply(avroRecord);
    return bytes;
  }

}
