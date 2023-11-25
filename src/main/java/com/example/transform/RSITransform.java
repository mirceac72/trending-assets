package com.example.transform;

import com.example.item.AssertRSI;
import com.example.item.AssetValue;
import com.example.item.CountAndChange;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import java.math.BigDecimal;

import static org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior.FIRE_ALWAYS;
import static org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior.FIRE_IF_NON_EMPTY;

public class RSITransform extends PTransform<PCollection<AssetValue>, PCollection<AssertRSI>> {

  @Override
  public PCollection<AssertRSI> expand(PCollection<AssetValue> input) {
    return input.apply("create-key-value-pairs", MapElements.into(
        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.bigdecimals()))
            .via(x -> KV.of(x.asset, x.value)))
        .apply("group-values-per-time-interval", Window.into(FixedWindows.of(Duration.standardHours(1))))
        .apply("sum-values-per-group", Combine.perKey(BigDecimal::add))
        .apply("calculate-change", new ChangeTransform(Duration.standardHours(1)))
        .apply("group-in-rs-period", Window.<KV<String, BigDecimal>>into(
            SlidingWindows.of(Duration.standardHours(14))
                .every(Duration.standardHours(1))
            )
            .triggering(AfterWatermark.pastEndOfWindow())
            .withAllowedLateness(Duration.ZERO, FIRE_IF_NON_EMPTY)
            .discardingFiredPanes()
        )
        .apply("compute-rsi", Combine.perKey(RSIFn.of(14)))
        .apply("filter-values-on-incomplete-periods", Filter.by(x -> x.getValue().compareTo(BigDecimal.ZERO) >= 0))
        .apply("create-asset-rsi", MapElements.into(TypeDescriptor.of(AssertRSI.class))
            .via(x -> new AssertRSI(x.getKey(), x.getValue()))).setCoder(AvroCoder.of(AssertRSI.class));
  }
}
