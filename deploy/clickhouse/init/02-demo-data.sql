CREATE DATABASE IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.events
(
    ts       DateTime,
    user_id  UInt64,
    event    LowCardinality(String),
    value    Float64
)
ENGINE = MergeTree
ORDER BY ts;

INSERT INTO demo.events
SELECT
    now() - (rand() % 86400) AS ts,
    rand() % 1000             AS user_id,
    ['view', 'click', 'buy'][1 + rand() % 3] AS event,
    rand() / 1000             AS value
FROM numbers(200000);
