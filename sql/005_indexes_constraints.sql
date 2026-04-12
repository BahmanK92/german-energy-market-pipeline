CREATE INDEX IF NOT EXISTS idx_raw_smard_timeseries_long_datetime_utc
    ON raw.smard_timeseries_long (datetime_utc);

CREATE INDEX IF NOT EXISTS idx_raw_smard_timeseries_long_value_name
    ON raw.smard_timeseries_long (value_name);

CREATE INDEX IF NOT EXISTS idx_raw_smard_timeseries_long_date_berlin
    ON raw.smard_timeseries_long (datetime_berlin);

CREATE INDEX IF NOT EXISTS idx_core_energy_hourly_date_berlin
    ON core.energy_hourly (date_berlin);

CREATE INDEX IF NOT EXISTS idx_core_energy_hourly_hour_of_day
    ON core.energy_hourly (hour_of_day);

CREATE INDEX IF NOT EXISTS idx_mart_energy_features_hourly_date_berlin
    ON mart.energy_features_hourly (date_berlin);

CREATE INDEX IF NOT EXISTS idx_mart_energy_features_hourly_hour_of_day
    ON mart.energy_features_hourly (hour_of_day);

CREATE INDEX IF NOT EXISTS idx_mart_energy_summary_daily_date_berlin
    ON mart.energy_summary_daily (date_berlin);