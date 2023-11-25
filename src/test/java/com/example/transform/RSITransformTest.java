package com.example.transform;

import com.example.item.AssertRSI;
import com.example.item.AssetValue;
import com.example.util.PrintTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

import static com.example.transform.TestUtils.withTs;
import static java.math.RoundingMode.UNNECESSARY;

class RSITransformTest {

  @Test
  public void testRSITransform() {

    var values = List.of(BigDecimal.valueOf(42.31), BigDecimal.valueOf(45.06), BigDecimal.valueOf(42.25),
        BigDecimal.valueOf(46.21), BigDecimal.valueOf(41.32), BigDecimal.valueOf(39.83), BigDecimal.valueOf(35.10),
        BigDecimal.valueOf(40.42), BigDecimal.valueOf(40.84), BigDecimal.valueOf(42.08), BigDecimal.valueOf(41.89),
        BigDecimal.valueOf(46.03), BigDecimal.valueOf(47.61), BigDecimal.valueOf(47.89), BigDecimal.valueOf(46.28));

    DateTime time = DateTime.now().withTimeAtStartOfDay();

    var testStreamBuilder = TestStream.create(AvroCoder.of(AssetValue.class));

    for (var value : values) {
      testStreamBuilder = testStreamBuilder
          .addElements(withTs(new AssetValue(time.toInstant(), "A", value)))
          .advanceWatermarkTo(time.toInstant());
      time = time.plusHours(1);
    }

    PTransform<PBegin, PCollection<AssetValue>> inputs = testStreamBuilder
        .advanceWatermarkToInfinity();

    Pipeline pipeline = Pipeline.create();

    var rsiCollection = pipeline.apply(inputs)
        .apply(new RSITransform());

    rsiCollection.apply(new PrintTransform<>());

    PAssert.that(rsiCollection).containsInAnyOrder(
        new AssertRSI("A", BigDecimal.valueOf(55.56).setScale(2, UNNECESSARY))
    );

    pipeline.run().waitUntilFinish();
  }
}