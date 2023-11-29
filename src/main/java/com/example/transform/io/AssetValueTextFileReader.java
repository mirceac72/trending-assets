package com.example.transform.io;

import com.example.item.AssetValue;
import com.example.item.AssetValueStr;
import com.example.item.Failure;
import com.example.transform.trend.AssetValueFn;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.List;

public class AssetValueTextFileReader extends PTransform<PBegin, PCollection<AssetValue>> {

  private final String inputFile;
  private final List<PCollection<Failure>> failureCollections;

  public AssetValueTextFileReader(String inputFile, List<PCollection<Failure>> failureCollections) {
    this.inputFile = inputFile;
    this.failureCollections = failureCollections;
  }

  @Override
  public PCollection<AssetValue> expand(PBegin input) {
    return input.apply("read-asset-values", read())
        .apply("parse-asset-values", MapElements.into(TypeDescriptor.of(AssetValue.class))
            .via(AssetValueFn.of())
            .exceptionsInto(TypeDescriptor.of(Failure.class))
            .exceptionsVia(exElement -> Failure.of("parse-asset-values", exElement.element().toString(),
                exElement.exception())))
        .failuresTo(failureCollections)
        .setCoder(AvroGenericCoder.of(AssetValue.class))
        .apply("assign-timestamps", new EventTimeTransform());
  }

  private PTransform<PBegin, PCollection<AssetValueStr>> read() {
    return AvroIO.read(AssetValueStr.class)
        .from(inputFile);
  }
}
