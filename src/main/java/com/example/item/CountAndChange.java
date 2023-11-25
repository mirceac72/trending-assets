package com.example.item;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import java.math.BigDecimal;

@DefaultSchema(JavaFieldSchema.class)
@DefaultCoder(AvroCoder.class)
public class CountAndChange{

  public CountAndChange() {
  }

  public static CountAndChange of(Integer count, BigDecimal change) {
    var cc = new CountAndChange();
    cc.count = count;
    cc.change = change;
    return cc;
  }

  public Integer count;
  public BigDecimal change;

}
