package com.example.transform.io;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Instant;

import java.text.MessageFormat;

/**
 * A PTransform that prints the elements of a PCollection to the console.
 * It is used for testing and debugging purposes.
 *
 * @param <T> the type of the elements in the input PCollection
 */
public class ConsoleWriter<T> extends PTransform<PCollection<T>, PDone> {
  @Override
  public PDone expand(PCollection<T> input) {
    input.apply("print-to-console", ParDo.of(new PrintFn<>()));
    return PDone.in(input.getPipeline());
  }

  private static class PrintFn<T> extends DoFn<T, Void> {
    @ProcessElement
    public void processElement(@Element T element, @Timestamp Instant ts)
    {
      var msg = MessageFormat.format("{0} {1}", ts.toDateTime(), element.toString());
      System.out.println(msg);
    }
  }
}
