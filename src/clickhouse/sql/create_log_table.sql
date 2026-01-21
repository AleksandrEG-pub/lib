CREATE OR REPLACE TABLE web_logs (
    timestamp DateTime,
    user_id UInt64,
    url String,
    response_time UInt32,
    status_code UInt16
) ENGINE = MergeTree()
ORDER BY (timestamp, user_id)
PARTITION BY toYYYYMM(timestamp);