CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS gold.census_mrts (
  time                  text            NOT NULL,
  dt                    date            NOT NULL,
  year                  int             NOT NULL,
  month                 int             NOT NULL,
  is_seasonally_adjusted boolean,
  category_code         text            NOT NULL,
  data_type_code        text            NOT NULL,
  time_slot_id          int,
  cell_value            double precision,
  error_data            text,
  ingest_date           date,
  PRIMARY KEY (dt, category_code, data_type_code, time_slot_id, is_seasonally_adjusted)
);

CREATE TABLE IF NOT EXISTS staging.census_mrts (
  time text,
  dt date,
  year int,
  month int,
  is_seasonally_adjusted boolean,
  category_code text,
  data_type_code text,
  time_slot_id int,
  cell_value double precision,
  error_data text,
  ingest_date date
);

CREATE TABLE IF NOT EXISTS gold.bls_series_history (
  series_id    text    NOT NULL,
  ts_ingested  timestamptz NOT NULL,
  dt           date    NOT NULL,
  year         int     NOT NULL,
  month        int     NOT NULL,
  period       text    NOT NULL,
  periodname   text    NULL,
  value        double precision NOT NULL,
  is_latest    boolean NOT NULL,
  footnotes    text[]  NULL,
  PRIMARY KEY (series_id, ts_ingested)
);

CREATE INDEX IF NOT EXISTS idx_bls_hist_dt_series
  ON gold.bls_series_history (dt, series_id);

CREATE OR REPLACE VIEW analytics.bls_latest AS
SELECT * FROM gold.bls_series_latest;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_census_latest AS
SELECT DISTINCT ON (category_code, data_type_code, time_slot_id, is_seasonally_adjusted)
  time, dt, year, month, is_seasonally_adjusted,
  category_code, data_type_code, time_slot_id,
  cell_value AS value, error_data, ingest_date
FROM gold.census_mrts
ORDER BY category_code, data_type_code, time_slot_id, is_seasonally_adjusted, dt DESC;

CREATE INDEX IF NOT EXISTS idx_mv_census_latest_key
  ON analytics.mv_census_latest (category_code, data_type_code, time_slot_id, is_seasonally_adjusted, dt DESC);

CREATE OR REPLACE VIEW analytics.census_series_monthly AS
SELECT
  dt, year, month,
  category_code, data_type_code, time_slot_id,
  is_seasonally_adjusted,
  cell_value AS value
FROM gold.census_mrts;

DROP TABLE IF EXISTS analytics.dim_date;
CREATE TABLE analytics.dim_date AS
SELECT
  d::date                           AS dt,
  EXTRACT(YEAR  FROM d)::int        AS year,
  EXTRACT(MONTH FROM d)::int        AS month,
  TO_CHAR(d, 'Mon')                 AS month_abbr,
  TO_CHAR(d, 'YYYY-MM')             AS year_month
FROM generate_series(
  (SELECT MIN(dt)                         FROM gold.census_mrts),
  (SELECT COALESCE(MAX(dt), CURRENT_DATE) FROM gold.census_mrts),
  interval '1 day'
) AS t(d);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_date_dt ON analytics.dim_date(dt);
GRANT SELECT ON analytics.dim_date TO pbi_read;

DO $$
BEGIN    --read-only for PowerBI
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pbi_read') THEN
    CREATE ROLE pbi_read LOGIN PASSWORD 'pbi_read';
  END IF;
END$$;

GRANT USAGE ON SCHEMA analytics, gold TO pbi_read;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics, gold TO pbi_read;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO pbi_read;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO pbi_read;