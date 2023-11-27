package com.example.transform.trend;

import com.example.item.AssetRSI;
import com.example.transform.io.ConsoleWriter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.IntStream;

import static com.example.transform.TestUtils.tkv;
import static com.example.transform.trend.RSIFn.*;
import static java.math.RoundingMode.UNNECESSARY;

class RSITransformTest {

  @Test
  public void testRSITransform() {

    var values = List.of(BigDecimal.valueOf(42.31), BigDecimal.valueOf(45.06), BigDecimal.valueOf(42.25),
        BigDecimal.valueOf(46.21), BigDecimal.valueOf(41.32), BigDecimal.valueOf(39.83), BigDecimal.valueOf(35.10),
        BigDecimal.valueOf(40.42), BigDecimal.valueOf(40.84), BigDecimal.valueOf(42.08), BigDecimal.valueOf(41.89),
        BigDecimal.valueOf(46.03), BigDecimal.valueOf(47.61), BigDecimal.valueOf(47.89), BigDecimal.valueOf(46.28));

    DateTime time = DateTime.now().withTimeAtStartOfDay();

    var testStreamBuilder = TestStream.create(KvCoder.of(StringUtf8Coder.of(), BigDecimalCoder.of()));

    for (var value : values) {
      testStreamBuilder = testStreamBuilder
          .addElements(tkv(time.toInstant(), "A", value))
          .advanceWatermarkTo(time.toInstant());
      time = time.plusHours(1);
    }

    PTransform<PBegin, PCollection<KV<String, BigDecimal>>> inputs = testStreamBuilder
        .advanceWatermarkToInfinity();

    Pipeline pipeline = Pipeline.create();

    var rsiCollection = pipeline.apply(inputs)
        .apply(new RSITransform());

    rsiCollection.apply(new ConsoleWriter<>());

    PAssert.that(rsiCollection).containsInAnyOrder(
        new AssetRSI("A", BigDecimal.valueOf(55.56).setScale(2, UNNECESSARY))
    );

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void computeRSIForPositiveChanges() {
    var values = IntStream.iterate(1, i -> i + 1)
        .limit(RSI_RECOMMENDED_PERIOD + 3)
        .mapToObj(BigDecimal::valueOf)
        .toList();

    DateTime time = DateTime.now().withTimeAtStartOfDay();

    var testStreamBuilder = TestStream.create(KvCoder.of(StringUtf8Coder.of(), BigDecimalCoder.of()));

    for (var value : values) {
      testStreamBuilder = testStreamBuilder
          .addElements(tkv(time.toInstant(), "A", value))
          .advanceWatermarkTo(time.toInstant());
      time = time.plusHours(1);
    }

    PTransform<PBegin, PCollection<KV<String, BigDecimal>>> inputs = testStreamBuilder
        .advanceWatermarkToInfinity();

    Pipeline pipeline = Pipeline.create();

    var rsiCollection = pipeline.apply(inputs)
        .apply(new RSITransform());

    rsiCollection.apply(new ConsoleWriter<>());

    PAssert.that(rsiCollection).containsInAnyOrder(
        new AssetRSI("A", RSI_MAX_VALUE),
        new AssetRSI("A", RSI_MAX_VALUE),
        new AssetRSI("A", RSI_MAX_VALUE)
    );

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void computeRSIForNegativeChanges() {
    var values = IntStream.iterate(-1, i -> i - 1)
        .limit(RSI_RECOMMENDED_PERIOD + 3)
        .mapToObj(BigDecimal::valueOf)
        .toList();

    DateTime time = DateTime.now().withTimeAtStartOfDay();

    var testStreamBuilder = TestStream.create(KvCoder.of(StringUtf8Coder.of(), BigDecimalCoder.of()));

    for (var value : values) {
      testStreamBuilder = testStreamBuilder
          .addElements(tkv(time.toInstant(), "A", value))
          .advanceWatermarkTo(time.toInstant());
      time = time.plusHours(1);
    }

    PTransform<PBegin, PCollection<KV<String, BigDecimal>>> inputs = testStreamBuilder
        .advanceWatermarkToInfinity();

    Pipeline pipeline = Pipeline.create();

    var rsiCollection = pipeline.apply(inputs)
        .apply(new RSITransform());

    rsiCollection.apply(new ConsoleWriter<>());

    PAssert.that(rsiCollection).containsInAnyOrder(
        new AssetRSI("A", RSI_MIN_VALUE),
        new AssetRSI("A", RSI_MIN_VALUE),
        new AssetRSI("A", RSI_MIN_VALUE)
    );

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void computeRSIForMixedChanges() {
    var values = IntStream.iterate(-1, i -> i * -1)
        .limit(RSI_RECOMMENDED_PERIOD + 3)
        .mapToObj(BigDecimal::valueOf)
        .toList();

    DateTime time = DateTime.now().withTimeAtStartOfDay();

    var testStreamBuilder = TestStream.create(KvCoder.of(StringUtf8Coder.of(), BigDecimalCoder.of()));

    for (var value : values) {
      testStreamBuilder = testStreamBuilder
          .addElements(tkv(time.toInstant(), "A", value))
          .advanceWatermarkTo(time.toInstant());
      time = time.plusHours(1);
    }

    PTransform<PBegin, PCollection<KV<String, BigDecimal>>> inputs = testStreamBuilder
        .advanceWatermarkToInfinity();

    Pipeline pipeline = Pipeline.create();

    var rsiCollection = pipeline.apply(inputs)
        .apply(new RSITransform());

    rsiCollection.apply(new ConsoleWriter<>());

    PAssert.that(rsiCollection).containsInAnyOrder(
        new AssetRSI("A", RSI_NEUTRAL_VALUE),
        new AssetRSI("A", RSI_NEUTRAL_VALUE),
        new AssetRSI("A", RSI_NEUTRAL_VALUE)
    );

    pipeline.run().waitUntilFinish();
  }
}