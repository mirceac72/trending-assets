package com.example.transform.trend;

import com.example.item.AssetValue;
import com.example.item.AssetValueStr;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Instant;

import java.math.BigDecimal;

public class AssetValueFn {

  public static SerializableFunction<AssetValueStr, AssetValue> of() {
    return assetValueStr -> new AssetValue(Instant.parse(assetValueStr.timestamp), assetValueStr.asset,
        new BigDecimal(assetValueStr.value));
  }
}
