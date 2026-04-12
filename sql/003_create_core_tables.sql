CREATE TABLE IF NOT EXISTS core.energy_hourly (
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

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);