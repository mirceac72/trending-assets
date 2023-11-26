package com.example.transform.io;

import com.example.item.AssetValue;
import com.example.item.AssetValueStr;
import com.example.transform.trend.AssetValueFn;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class AssetValueTextFileReader extends PTransform<PBegin, PCollection<AssetValue>> {

  private String inputFile;

  public AssetValueTextFileReader(String inputFile) {
    this.inputFile = inputFile;
  }

  @Override
  public PCollection<AssetValue> expand(PBegin input) {
    return input.apply("read-asset-values", read())
        .apply("parse-asset-values", MapElements.into(TypeDescriptor.of(AssetValue.class))
            .via(AssetValueFn.of()))
        .setCoder(AvroGenericCoder.of(AssetValue.class))
        .apply("assign-timestamps", new EventTimeTransform());
  }

  private PTransform<PBegin, PCollection<AssetValueStr>> read() {
    return AvroIO.read(AssetValueStr.class)
        .from(inputFile);
  }
}
