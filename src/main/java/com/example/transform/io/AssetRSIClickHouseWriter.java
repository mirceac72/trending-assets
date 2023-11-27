package com.example.transform.io;

import com.example.item.AssetRSI;
import org.apache.beam.sdk.io.clickhouse.ClickHouseIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

public class AssetRSIClickHouseWriter extends PTransform<PCollection<AssetRSI>, PDone> {
  private String jdbcUrl;
  private String tableName;

  public AssetRSIClickHouseWriter(String jdbcUrl, String tableName) {
    this.jdbcUrl = jdbcUrl;
    this.tableName = tableName;
  }

  @Override
  public PDone expand(PCollection<AssetRSI> input) {
    return input.apply("convert-to-row", ParDo.of(new AssetRSIRowFn()))
        .setRowSchema(AssetRSIRowFn.assetRSISchema)
        .apply("write-to-clickhouse", ClickHouseIO.write(jdbcUrl, tableName));
  }

  private static class AssetRSIRowFn extends DoFn<AssetRSI, Row> {

    private static final Schema assetRSISchema = Schema.builder()
        .addDateTimeField("timestamp")
        .addStringField("asset")
        .addDoubleField("rsi")
        .build();

    @ProcessElement
    public void processElement(@Element AssetRSI assetRSI, @Timestamp Instant ts, OutputReceiver<Row> outputReceiver) {
      var row = Row.withSchema(assetRSISchema)
          .addValue(ts.toDateTime())
          .addValue(assetRSI.asset)
          .addValue(assetRSI.rsi.doubleValue())
          .build();
      outputReceiver.output(row);
    }
  }
}
