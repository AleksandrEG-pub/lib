DROP VIEW IF EXISTS error_counts_mv;

CREATE MATERIALIZED VIEW error_counts_mv
ENGINE = SummingMergeTree()
ORDER BY (date, status_code_group)
POPULATE
AS SELECT
    toDate(timestamp) AS date,
    intDiv(status_code, 100) AS status_code_group,  -- 4xx, 5xx groups
    count() AS error_count
FROM web_logs
WHERE status_code >= 400
GROUP BY date, status_code_group;