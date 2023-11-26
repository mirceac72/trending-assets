package com.example.transform.io;

import com.example.item.AssetValue;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.kafka.CustomTimestampPolicyWithLimitedDelay;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.joda.time.Duration;

public class AssetValueReader {
  public PTransform<PBegin, PCollection<AssetValue>> build() {
    return KafkaIO.<Long, AssetValue>read()
        .withBootstrapServers("localhost:9092")
        .withTopic("asset-value")
        .withKeyDeserializer(LongDeserializer.class)
        .withValueDeserializerAndCoder((Class) KafkaAvroDeserializer.class, AvroCoder.of(AssetValue.class))
        .withTimestampPolicyFactory((topicPartition, previousWatermark) ->
            new CustomTimestampPolicyWithLimitedDelay<Long, AssetValue>(kafkaRecord ->
                kafkaRecord.getKV().getValue().timestamp, Duration.standardMinutes(5), previousWatermark));
  };
}
