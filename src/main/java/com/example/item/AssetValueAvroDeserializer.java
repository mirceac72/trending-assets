package com.example.item;

import com.example.item.AssetValue;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import org.springframework.core.serializer.Deserializer;

import java.io.IOException;
import java.io.InputStream;

public class AssetValueAvroDeserializer extends AbstractKafkaAvroDeserializer implements Deserializer<AssetValue> {
  @Override
  public AssetValue deserialize(InputStream inputStream) throws IOException {
    return null;
  }

  @Override
  public AssetValue deserializeFromByteArray(byte[] serialized) throws IOException {
    return Deserializer.super.deserializeFromByteArray(serialized);
  }
}
