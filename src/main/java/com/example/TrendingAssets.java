package com.example;

import com.example.item.AssertRSI;
import com.example.item.AssetValue;
import com.example.transform.io.AssetValueKafkaReader;
import com.example.transform.io.AssetValueTextFileReader;
import com.example.transform.trend.RSITransform;
import com.example.transform.util.PrintTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class TrendingAssets {

  public static void main(String[] args) {
    var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    var pipeline = Pipeline.create(options);
		TrendingAssets.buildPipeline(pipeline, options);
    pipeline.run().waitUntilFinish();
  }

  public interface Options extends StreamingOptions {

    @Description("Input type (file and kafka are supported)")
    @Default.Enum("FILE")
    InputType getInputType();
    void setInputType(InputType inputType);

    @Description("Input file path. Used only if input type is file")
    @Default.String("resources/data_file.avro")
    String getInputFile();
    void setInputFile(String inputFile);

    @Description("Kafka bootstrap servers. Used only if input type is kafka")
    @Default.String("localhost:9092")
    String getKafkaBootstrapServers();
    void setKafkaBootstrapServers(String kafkaBootstrapServers);

    @Description("Kafka topic. Used only if input type is kafka")
    @Default.String("asset-value")
    String getKafkaTopic();
    void setKafkaTopic(String kafkaTopic);

    @Description("Output type (console and clickhouse are supported)")
    @Default.Enum("CONSOLE")
    OutputType getOutputType();
    void setOutputType(OutputType outputType);

    @Description("Relative Strength Index Period")
    @Default.Integer(14)
    Integer getRsiPeriod();
    void setRsiPeriod(Integer period);
  }

  public enum InputType {
    FILE,
    KAFKA
  }

  public enum OutputType {
    CONSOLE,
    CLICKHOUSE
  }

  private static void buildPipeline(Pipeline pipeline, Options options) {

    pipeline.apply("read-asset-values", readerTransform(options))
        .apply("calculate-rsi", (new RSITransform(options.getRsiPeriod())))
        .apply("write-rsi", writerTransform(options));
  }

  private static PTransform<PBegin, PCollection<AssetValue>> readerTransform(Options options) {

    return switch (options.getInputType()) {
      case FILE -> new AssetValueTextFileReader(options.getInputFile());
      case KAFKA -> new AssetValueKafkaReader(options.getKafkaBootstrapServers(), options.getKafkaTopic());
    };
  }

  private static PTransform<PCollection<AssertRSI>, PDone> writerTransform(Options options) {

    return switch (options.getOutputType()) {
      case CONSOLE -> new PrintTransform<>();
      default -> throw new IllegalArgumentException("Unsupported output type: " + options.getOutputType());
    };
  }
}
