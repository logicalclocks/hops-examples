package com.logicalclocks.flink.functions;

import com.logicalclocks.flink.schemas.SourceTransaction;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AggregateRichWindowFunction extends RichWindowFunction<SourceTransaction, byte[], Long, TimeWindow> {

  // descriptive statistics
  private DescriptiveStatistics descriptiveStatistics;

  // fields of the feature group
  private List<String> fields;

  // Avro schema in JSON format.
  private final String schemaString;

  // Cannot be serialized so we create these in open().
  private transient Schema schema;
  private transient GenericData.Record record;

  public AggregateRichWindowFunction(Schema schema, List<String> fields) {
    this.schemaString = schema.toString();
    this.fields = fields;
  }

  // change this function according to your needs. In the next release users will be able to provide config file
  @Override
  public void apply(Long key, TimeWindow timeWindow, Iterable<SourceTransaction> iterable, Collector<byte[]> collector)
      throws Exception {

    long cnt = 0;
    for (SourceTransaction r : iterable) {
      cnt++;
      descriptiveStatistics.addValue(r.getAmount());
    }

    double avg = descriptiveStatistics.getSum() / cnt;

    record.put(fields.get(0), key);
    record.put(fields.get(1), cnt);
    record.put(fields.get(2), avg);
    record.put(fields.get(3), descriptiveStatistics.getStandardDeviation());

    collector.collect(encode(record));
  }

  @Override
  public void open(Configuration parameters) {
    this.descriptiveStatistics = new DescriptiveStatistics();
    this.schema = new Schema.Parser().parse(this.schemaString);
    this.record = new GenericData.Record(this.schema);
  }

  private byte[] encode(GenericRecord record) throws IOException {
    List<GenericRecord> records = new ArrayList<>();
    records.add(record);

    GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    byteArrayOutputStream.reset();
    BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
    for(GenericRecord segment: records) {
      datumWriter.write(segment, binaryEncoder);
    }
    binaryEncoder.flush();
    byte[] bytes = byteArrayOutputStream.toByteArray();
    return bytes;
  }
}
