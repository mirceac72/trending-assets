package com.example;

import com.example.item.AssetRSI;
import com.example.item.AssetValue;
import com.example.transform.io.AssetRSIClickHouseWriter;
import com.example.transform.io.AssetValueKafkaReader;
import com.example.transform.io.AssetValueTextFileReader;
import com.example.transform.trend.RSITransform;
import com.example.transform.io.ConsoleWriter;
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
    @Default.Enum("file")
    InputType getInputType();
    void setInputType(InputType inputType);

    @Description("Input file path. Used only if input type is file")
    @Default.String("resources/asset-value-increasing.avro")
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
    @Default.Enum("console")
    OutputType getOutputType();
    void setOutputType(OutputType outputType);

    @Description("Clickhouse JDBC URL. Used only if output type is clickhouse")
    @Default.String("jdbc:clickhouse://localhost:8123/default?user=default&password=")
    String getJdbcUrl();
    void setJdbcUrl(String jdbcUrl);

    @Description("Clickhouse table name. Used only if output type is clickhouse")
    @Default.String("asset_rsi")
    String getTableName();
    void setTableName(String tableName);

    @Description("Relative Strength Index Period")
    @Default.Integer(14)
    Integer getRsiPeriod();
    void setRsiPeriod(Integer period);
  }

  public enum InputType {
    file,
    kafka
  }

  public enum OutputType {
    console,
    clickhouse
  }

  private static void buildPipeline(Pipeline pipeline, Options options) {

    pipeline.apply("read-asset-values", readerTransform(options))
        .apply("calculate-rsi", (new RSITransform(options.getRsiPeriod())))
        .apply("write-rsi", writerTransform(options));
  }

  private static PTransform<PBegin, PCollection<AssetValue>> readerTransform(Options options) {

    return switch (options.getInputType()) {
      case file -> new AssetValueTextFileReader(options.getInputFile());
      case kafka -> new AssetValueKafkaReader(options.getKafkaBootstrapServers(), options.getKafkaTopic());
    };
  }

  private static PTransform<PCollection<AssetRSI>, PDone> writerTransform(Options options) {

    return switch (options.getOutputType()) {
      case console -> new ConsoleWriter<>();
      case clickhouse -> new AssetRSIClickHouseWriter(options.getJdbcUrl(), options.getTableName());
    };
  }
}
