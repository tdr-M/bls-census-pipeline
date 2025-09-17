CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS staging;

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