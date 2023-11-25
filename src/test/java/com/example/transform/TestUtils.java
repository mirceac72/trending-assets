package com.example.transform;

import com.example.item.AssetValue;
import org.apache.beam.sdk.values.TimestampedValue;

public final class TestUtils {

  private TestUtils() {
  }

  public static TimestampedValue<AssetValue> withTs(AssetValue av) {
    return TimestampedValue.of(av, av.timestamp);
  }
}
