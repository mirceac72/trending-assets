CREATE TABLE IF NOT EXISTS asset_rsi (
  timestamp DateTime NOT NULL,
  asset String NOT NULL,
  rsi Double NOT NULL
) Engine = ReplacingMergeTree() ORDER BY (timestamp, asset);