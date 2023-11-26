package com.example.transform.util;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * A PTransform that prints the elements of a PCollection to the console.
 * It is used for testing and debugging purposes.
 *
 * @param <T> the type of the elements in the input PCollection
 */
public class PrintTransform<T> extends PTransform<PCollection<T>, PDone> {
  @Override
  public PDone expand(PCollection<T> input) {
    input.apply("print-to-console", ParDo.of(new PrintFn<>()));
    return PDone.in(input.getPipeline());
  }

  private static class PrintFn<T> extends DoFn<T, Void> {
    @ProcessElement
    public void processElement(@Element T element) {
      System.out.println(element.toString());
    }
  }
}
