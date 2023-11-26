package com.example.item;

public class AssetValueStr {

  public AssetValueStr() {
  }

  public AssetValueStr(String timestamp, String asset, String value) {
    this.timestamp = timestamp;
    this.asset = asset;
    this.value = value;
  }

  public String timestamp;
  public String asset;
  public String value;
}
