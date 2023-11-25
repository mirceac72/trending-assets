package com.example.item;

import org.joda.time.Instant;

import java.math.BigDecimal;

public class AssetValue {

  public AssetValue() {
  }

  public AssetValue(Instant timestamp, String asset, BigDecimal value) {
    this.timestamp = timestamp;
    this.asset = asset;
    this.value = value;
  }

  public Instant timestamp;
  public String asset;
  public BigDecimal value;
}
