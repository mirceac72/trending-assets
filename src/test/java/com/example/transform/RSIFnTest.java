package com.example.transform;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.stream.DoubleStream;

import static com.example.transform.RSIFn.*;
import static java.math.RoundingMode.UNNECESSARY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RSIFnTest {

  private static final BigDecimal POSITIVE_TREND_THRESHOLD = BigDecimal.valueOf(70);
  private static final BigDecimal NEGATIVE_TREND_THRESHOLD = BigDecimal.valueOf(30);
  private static final BigDecimal RSI_MIN_VALUE = BigDecimal.valueOf(0).setScale(2, UNNECESSARY);
  private static final BigDecimal RSI_NEUTRAL_VALUE = BigDecimal.valueOf(50).setScale(2, UNNECESSARY);

  @Test
  public void computeRSIForPositiveChanges() {
    var rsiFn = RSIFn.of(RECOMMENDED_PERIOD);
    var rsi = rsiFn.apply(DoubleStream.iterate(1.0, d -> d)
        .limit(RECOMMENDED_PERIOD)
        .mapToObj(BigDecimal::valueOf)
        .toList());

    assertEquals(RSI_MAX_VALUE, rsi);
  }

  @Test
  public void computeRSIForNegativeChanges() {
    var rsiFn = RSIFn.of(RECOMMENDED_PERIOD);
    var rsi = rsiFn.apply(DoubleStream.iterate(-1.0, d -> d)
        .limit(RECOMMENDED_PERIOD)
        .mapToObj(BigDecimal::valueOf)
        .toList());

    assertEquals(RSI_MIN_VALUE, rsi);
  }

  @Test
  public void computeRSIForMixedChanges() {
    var rsiFn = RSIFn.of(RECOMMENDED_PERIOD);
    var rsi = rsiFn.apply(DoubleStream.iterate(-1.0, d -> d * -1)
        .limit(RECOMMENDED_PERIOD)
        .mapToObj(BigDecimal::valueOf)
        .toList());

    assertEquals(RSI_NEUTRAL_VALUE, rsi);
  }

  @Test
  public void computeRSIForNoChanges() {
    var rsiFn = RSIFn.of(RECOMMENDED_PERIOD);
    var rsi = rsiFn.apply(DoubleStream.iterate(0.0, d -> d)
        .limit(RECOMMENDED_PERIOD)
        .mapToObj(BigDecimal::valueOf)
        .toList());

    assertEquals(RSI_MAX_VALUE, rsi);
  }

  @Test
  public void computeRSIForMainlyPositiveChanges() {
    var rsiFn = RSIFn.of(RECOMMENDED_PERIOD);
    var rsi = rsiFn.apply(DoubleStream.iterate(12.0, d -> d - 1)
        .limit(RECOMMENDED_PERIOD)
        .mapToObj(BigDecimal::valueOf)
        .toList());

    assertTrue(rsi.compareTo(POSITIVE_TREND_THRESHOLD) > 0);
  }

  @Test
  public void computeRSIForMainlyNegativeChanges() {
    var rsiFn = RSIFn.of(RECOMMENDED_PERIOD);
    var rsi = rsiFn.apply(DoubleStream.iterate(-12.0, d -> d + 1)
        .limit(RECOMMENDED_PERIOD)
        .mapToObj(BigDecimal::valueOf)
        .toList());

    assertTrue(rsi.compareTo(NEGATIVE_TREND_THRESHOLD) < 0);
  }

  @Test
  public void computeRSIForLessChangesThanPeriod() {
    var rsiFn = RSIFn.of(RECOMMENDED_PERIOD);
    var rsi = rsiFn.apply(DoubleStream.iterate(1.0, d -> d)
        .limit(7)
        .mapToObj(BigDecimal::valueOf)
        .toList());

    assertEquals(RSI_ERROR, rsi);
  }

  @Test
  public void computeRSIForMoreChangesThanPeriod() {
    var rsiFn = RSIFn.of(RECOMMENDED_PERIOD);
    var rsi = rsiFn.apply(DoubleStream.iterate(1.0, d -> d)
        .limit(15)
        .mapToObj(BigDecimal::valueOf)
        .toList());

    assertEquals(BigDecimal.valueOf(-1), rsi);
  }
}