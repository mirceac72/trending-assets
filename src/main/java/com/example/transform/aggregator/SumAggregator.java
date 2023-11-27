package com.example.transform.aggregator;

import com.example.item.AssetValue;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import java.math.BigDecimal;

/**
 * Aggregates the values of the assets from the input PCollection over a provided interval of time.
 * It creates an output PCollection of KV<String, BigDecimal> where the keys are the assets from the input
 * and the values are the aggregated values of the assets over the provided window.
 */
public class SumAggregator extends PTransform<PCollection<AssetValue>, PCollection<KV<String, BigDecimal>>> {

  private final Duration inputPeriod;

  /**
   * Aggregates the values of the assets from the input PCollection over the provided interval of time.
   * @param inputPeriod - the interval of time over which the values of the assets from the input PCollection
   *                    are aggregated
   */
  public SumAggregator(Duration inputPeriod) {
    this.inputPeriod = inputPeriod;
  }

  @Override
  public PCollection<KV<String, BigDecimal>> expand(PCollection<AssetValue> input) {
    return input.apply("create-key-value-pairs", MapElements.into(
                TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.bigdecimals()))
            .via(assetValue -> KV.of(assetValue.asset, assetValue.value)))
        .apply("group-values-per-time-interval", Window.into(FixedWindows.of(inputPeriod)))
        .apply("sum-values-per-group", Combine.perKey(BigDecimal::add));
  }
}
