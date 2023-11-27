package com.example.item;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import java.math.BigDecimal;
import java.util.Objects;

@DefaultSchema(JavaFieldSchema.class)
@DefaultCoder(AvroCoder.class)
public class AssetRSI {

    public AssetRSI() {
    }

    public AssetRSI(String asset, BigDecimal rsi) {
        this.asset = asset;
        this.rsi = rsi;
    }

    public String asset;
    public BigDecimal rsi;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AssetRSI assetRSI = (AssetRSI) o;
        return Objects.equals(asset, assetRSI.asset) && Objects.equals(rsi, assetRSI.rsi);
    }

    @Override
    public int hashCode() {
        return Objects.hash(asset, rsi);
    }

    @Override
    public String toString() {
        return "AssertRSI{" +
            "asset='" + asset + '\'' +
            ", rsiValue=" + rsi +
            '}';
    }
}
