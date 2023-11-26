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

public class AssetValueKafkaReader extends PTransform<PBegin, PCollection<AssetValue>> {

  private String bootstrapServers;
  private String topic;

  public AssetValueKafkaReader(String bootstrapServers, String topic) {
    this.bootstrapServers = bootstrapServers;
    this.topic = topic;
  }

  @Override
  public PCollection<AssetValue> expand(PBegin input) {
    return input.apply("from-kafka", read());
  }

  private PTransform<PBegin, PCollection<AssetValue>> read() {
    return KafkaIO.<Long, AssetValue>read()
        .withBootstrapServers(bootstrapServers)
        .withTopic(topic)
        .withKeyDeserializer(LongDeserializer.class)
        .withValueDeserializerAndCoder((Class) KafkaAvroDeserializer.class, AvroCoder.of(AssetValue.class))
        .withTimestampPolicyFactory((topicPartition, previousWatermark) ->
            new CustomTimestampPolicyWithLimitedDelay<Long, AssetValue>(kafkaRecord ->
                kafkaRecord.getKV().getValue().timestamp, Duration.standardMinutes(5), previousWatermark));
  };
}
