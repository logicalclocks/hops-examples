package com.logicalclocks.flink.schemas;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class SourceTransactionSchema implements DeserializationSchema<SourceTransaction>,
  SerializationSchema<SourceTransaction> {

  private static final long serialVersionUID = 1L;

  @Override
  public SourceTransaction deserialize(byte[] message) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(message, SourceTransaction.class);
  }

  @Override
  public boolean isEndOfStream(SourceTransaction fxData) {
    return false;
  }

  @Override
  public byte[] serialize(SourceTransaction fxData) {
    return fxData.toString().getBytes();
  }

  @Override
  public TypeInformation<SourceTransaction> getProducedType() {
    return TypeInformation.of(SourceTransaction.class);
  }
}
