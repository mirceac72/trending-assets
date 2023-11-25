package com.example;

import com.example.transform.AssetValueReader;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;

public class TrendingAssets {
  public static void buildPipeline(Pipeline pipeline, Integer rsiPeriod) {

    pipeline.apply("Read asset values", (new AssetValueReader()).build());
  }

  public static void main(String[] args) {
    var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    var pipeline = Pipeline.create(options);
		TrendingAssets.buildPipeline(pipeline, options.getRsiPeriod());
    pipeline.run().waitUntilFinish();
  }

  public interface Options extends StreamingOptions {
    @Description("Relative Strength Index Period")
    @Default.Integer(14)
    Integer getRsiPeriod();

    void setRsiPeriod(Integer period);
  }
}
