package com.example.item;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import java.math.BigDecimal;
import java.util.Objects;

@DefaultSchema(JavaFieldSchema.class)
@DefaultCoder(AvroCoder.class)
public class AssertRSI{

    public AssertRSI() {
    }

    public AssertRSI(String asset, BigDecimal rsiValue) {
        this.asset = asset;
        this.rsiValue = rsiValue;
    }

    public String asset;
    public BigDecimal rsiValue;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AssertRSI assertRSI = (AssertRSI) o;
        return Objects.equals(asset, assertRSI.asset) && Objects.equals(rsiValue, assertRSI.rsiValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(asset, rsiValue);
    }

    @Override
    public String toString() {
        return "AssertRSI{" +
            "asset='" + asset + '\'' +
            ", rsiValue=" + rsiValue +
            '}';
    }
}
