package com.example.transform.trend;

import org.apache.beam.sdk.transforms.SerializableFunction;

import java.math.BigDecimal;

import static java.math.RoundingMode.HALF_UP;
import static java.math.RoundingMode.UNNECESSARY;

public final class RSIFn {

  public static final Integer RSI_RECOMMENDED_PERIOD = 14;

  /**
   * Maximum value for the Relative Strength Index (RSI). This is the value for a strongly positive trend.
   */
  public static final BigDecimal RSI_MAX_VALUE = BigDecimal.valueOf(100).setScale(2, UNNECESSARY);
  /**
   * Minimum value for the Relative Strength Index (RSI). This is the value for a strongly negative trend.
   */
  public static final BigDecimal RSI_MIN_VALUE = BigDecimal.valueOf(0).setScale(2, UNNECESSARY);
  /**
   * Neutral value for the Relative Strength Index (RSI). This is the value for a neutral (neither positive nor negative)
   * trend.
   */
  public static final BigDecimal RSI_NEUTRAL_VALUE = BigDecimal.valueOf(50).setScale(2, UNNECESSARY);
  /**
   * Indicates that the RSI value could not be calculated over the change set. This can happen if the
   * change set has less or more values than the RSI period.
   */
  public static final BigDecimal RSI_ERROR = BigDecimal.valueOf(-1);

  private RSIFn() {
  }

  /**
   * Calculates the Relative Strength Index (RSI) value for a given set of changes.
   * <p>
   *  RSI = 100 - 100 / (1 + RS)
   * </p>
   *  where
   * <p>
   *  RS = Average of positive changes over the entire change set (average gain) /
   *  Average of negative changes (absolute values) over the entire change set (average loss)
   * </p>
   * @param period the number of changes to consider in order to calculate the RSI value
   *
   * @return The computed RSI value if the number of changes is equal to period, -1 otherwise
   */
  public static SerializableFunction<Iterable<BigDecimal>, BigDecimal> of(Integer period) {
    return changes -> {
      BigDecimal up = BigDecimal.ZERO;
      BigDecimal down = BigDecimal.ZERO;
      int count = 0;
      for (BigDecimal value : changes) {
        count++;
        if (count > period) {
          break;
        }
        if (value.compareTo(BigDecimal.ZERO) > 0) {
          up = up.add(value);
        } else {
          down = down.add(value.abs());
        }
      }
      if (count != period) {
        return RSI_ERROR;
      }
      if (down.compareTo(BigDecimal.ZERO) == 0) {
        return RSI_MAX_VALUE;
      }
      BigDecimal rs = up.divide(down, 2, HALF_UP);
      return BigDecimal.valueOf(100).subtract(BigDecimal.valueOf(100).divide(BigDecimal.ONE.add(rs), 2,
          HALF_UP));
    };
  }
}
