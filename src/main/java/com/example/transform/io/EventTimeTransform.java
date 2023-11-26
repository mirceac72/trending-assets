package com.example.transform.io;

import com.example.item.AssetValue;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

public class EventTimeTransform extends PTransform<PCollection<AssetValue>, PCollection<AssetValue>> {
  @Override
  public PCollection<AssetValue> expand(PCollection<AssetValue> input) {
    return input.apply(WithTimestamps.of(av -> av.timestamp));
  }
}
