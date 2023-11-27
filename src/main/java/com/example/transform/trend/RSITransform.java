package com.example.transform.trend;

import com.example.item.AssetRSI;
import com.example.item.AssetValue;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import javax.xml.crypto.dsig.keyinfo.KeyValue;
import java.math.BigDecimal;

import static com.example.transform.trend.RSIFn.RSI_RECOMMENDED_PERIOD;
import static org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior.FIRE_IF_NON_EMPTY;

public class RSITransform extends PTransform<PCollection<KV<String, BigDecimal>>, PCollection<AssetRSI>> {

  private Duration inputPeriod;
  private int rsiPeriod;

  public RSITransform() {
    this(Duration.standardHours(1), RSI_RECOMMENDED_PERIOD);
  }

  public RSITransform(Duration inputPeriod, int rsiPeriod) {

    this.inputPeriod = inputPeriod;
    this.rsiPeriod = rsiPeriod;
  }

  @Override
  public PCollection<AssetRSI> expand(PCollection<KV<String, BigDecimal>> input) {
    return input
        .apply("calculate-change", new ChangeTransform(inputPeriod))
        .apply("group-in-rs-period", Window.<KV<String, BigDecimal>>into(
            SlidingWindows.of(inputPeriod.multipliedBy(rsiPeriod))
                .every(inputPeriod)
            )
            .triggering(AfterWatermark.pastEndOfWindow())
            .withAllowedLateness(Duration.ZERO, FIRE_IF_NON_EMPTY)
            .discardingFiredPanes()
        )
        .apply("compute-rsi", Combine.perKey(RSIFn.of(rsiPeriod)))
        .apply("filter-values-on-incomplete-rsi-periods", Filter.by(x -> x.getValue().compareTo(BigDecimal.ZERO) >= 0))
        .apply("create-asset-rsi", MapElements.into(TypeDescriptor.of(AssetRSI.class))
            .via(x -> new AssetRSI(x.getKey(), x.getValue()))).setCoder(AvroCoder.of(AssetRSI.class));
  }
}
