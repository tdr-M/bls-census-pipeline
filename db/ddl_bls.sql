CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS gold.bls_series_latest (
  series_id    text PRIMARY KEY,
  ts_ingested  timestamptz NOT NULL,
  dt           date        NOT NULL,
  year         int         NOT NULL,
  month        int         NOT NULL,
  period       text        NOT NULL,
  periodname   text        NULL,
  value        double precision NOT NULL,
  is_latest    boolean     NOT NULL,
  footnotes    text[]      NULL
);
CREATE INDEX IF NOT EXISTS idx_bls_latest_dt ON gold.bls_series_latest (dt);

CREATE TABLE IF NOT EXISTS gold.bls_series_history (
  series_id    text         NOT NULL,
  ts_ingested  timestamptz  NOT NULL,
  dt           date         NOT NULL,
  year         int          NOT NULL,
  month        int          NOT NULL,
  period       text         NOT NULL,
  periodname   text         NULL,
  value        double precision NOT NULL,
  is_latest    boolean      NOT NULL,
  footnotes    text[]       NULL,
  PRIMARY KEY (series_id, ts_ingested)
);
CREATE INDEX IF NOT EXISTS idx_bls_hist_dt_series
  ON gold.bls_series_history (dt, series_id);

CREATE OR REPLACE VIEW analytics.bls_latest AS
SELECT * FROM gold.bls_series_latest;

GRANT USAGE ON SCHEMA analytics, gold TO pbi_read;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics, gold TO pbi_read;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO pbi_read;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold      GRANT SELECT ON TABLES TO pbi_read;