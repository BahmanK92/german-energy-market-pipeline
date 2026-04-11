CREATE TABLE IF NOT EXISTS raw.smard_timeseries_long (
    series_key TEXT NOT NULL,
    value_name TEXT NOT NULL,
    region TEXT NOT NULL,
    resolution TEXT NOT NULL,
    datetime_utc TIMESTAMPTZ NOT NULL,
    datetime_berlin TIMESTAMP NOT NULL,
    value DOUBLE PRECISION,
    source_batch_timestamp BIGINT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_raw_smard_timeseries_long
        PRIMARY KEY (series_key, datetime_utc)
);