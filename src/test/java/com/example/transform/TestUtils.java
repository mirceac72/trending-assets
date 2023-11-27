package com.example.transform;

import com.example.item.AssetValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;

import java.math.BigDecimal;

public final class TestUtils {

  private TestUtils() {
  }

  public static TimestampedValue<AssetValue> withTs(AssetValue av) {
    return TimestampedValue.of(av, av.timestamp);
  }

  public static TimestampedValue<KV<String, BigDecimal>> tkv(Instant ts, String asset, BigDecimal value) {
    return TimestampedValue.of(KV.of(asset, value), ts);
  }
}
