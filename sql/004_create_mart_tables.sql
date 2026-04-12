CREATE TABLE IF NOT EXISTS mart.energy_features_hourly (
    datetime_utc TIMESTAMPTZ PRIMARY KEY,
    datetime_berlin TIMESTAMP NOT NULL,
    date_berlin DATE NOT NULL,
    hour_of_day INTEGER NOT NULL,

    price_eur_mwh DOUBLE PRECISION,
    load_mw DOUBLE PRECISION,
    residual_load_smard_mw DOUBLE PRECISION,

    solar_mw DOUBLE PRECISION,
    wind_onshore_mw DOUBLE PRECISION,
    wind_offshore_mw DOUBLE PRECISION,
    biomass_mw DOUBLE PRECISION,
    hydro_mw DOUBLE PRECISION,

    lignite_mw DOUBLE PRECISION,
    hard_coal_mw DOUBLE PRECISION,
    gas_mw DOUBLE PRECISION,
    other_conventional_mw DOUBLE PRECISION,

    coal_mw DOUBLE PRECISION,
    renewable_generation_mw DOUBLE PRECISION,
    fossil_generation_mw DOUBLE PRECISION,
    total_generation_selected_mw DOUBLE PRECISION,
    renewable_share DOUBLE PRECISION,
    fossil_share DOUBLE PRECISION,
    residual_load_mw DOUBLE PRECISION,
    day_of_week INTEGER,
    is_weekend BOOLEAN,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mart.energy_summary_daily (
    date_berlin DATE PRIMARY KEY,

    avg_price_eur_mwh DOUBLE PRECISION,
    min_price_eur_mwh DOUBLE PRECISION,
    max_price_eur_mwh DOUBLE PRECISION,

    avg_load_mw DOUBLE PRECISION,
    total_load_mwh_est DOUBLE PRECISION,

    avg_residual_load_mw DOUBLE PRECISION,
    avg_residual_load_smard_mw DOUBLE PRECISION,

    avg_renewable_generation_mw DOUBLE PRECISION,
    avg_fossil_generation_mw DOUBLE PRECISION,
    avg_renewable_share DOUBLE PRECISION,
    avg_fossil_share DOUBLE PRECISION,

    avg_coal_mw DOUBLE PRECISION,
    avg_gas_mw DOUBLE PRECISION,

    row_count_hours INTEGER,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);