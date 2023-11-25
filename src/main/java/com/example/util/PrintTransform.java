package com.example.util;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

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
