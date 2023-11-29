package com.example.item;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaFieldSchema.class)
@DefaultCoder(AvroGenericCoder.class)
public class Failure {

  public String step;
  public String element;
  public Throwable exception;
  public Failure() {
  }

  public static Failure of(String step, String element, Throwable exception) {
    Failure failure = new Failure();
    failure.step = step;
    failure.element = element;
    failure.exception = exception;
    return failure;
  }

  @Override
  public String toString() {
    return "Failure{" +
        "step='" + step + '\'' +
        ", element='" + element + '\'' +
        ", exception=" + exception +
        '}';
  }
}
