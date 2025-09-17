from __future__ import annotations
import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from jobs.utils.config_loader import load_yaml
from jobs.utils.logging_setup import setup_logging
from jobs.utils.spark_session import get_spark

LOG = logging.getLogger("jobs.census_ingest")

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def fetch_census_payload(base_url: str, dataset_path: str, params: dict) -> list[list[str]]:
    url = f"{base_url.rstrip('/')}/{dataset_path.lstrip('/')}"
    LOG.info("Requesting Census API: %s params=%s", url, params)
    r = requests.get(url, params=params, timeout=30)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        preview = r.text[:300].replace("\n", " ")
        raise requests.HTTPError(f"{e} | body: {preview}") from e
    payload = r.json()
    if not isinstance(payload, list) or not payload:
        raise ValueError("Unexpected Census API response shape.")
    return payload

def normalize_tabular_json(rows: list[list[str]]) -> list[dict]:
    header, *data = rows
    return [dict(zip(header, row)) for row in data]

def write_jsonl(records: list[dict], path: Path) -> None:
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")

def to_bronze_parquet(jsonl_path: Path, bronze_dir: Path) -> None:
    spark = get_spark("census-bronze")
    df = spark.read.json(str(jsonl_path))
    today = datetime.now(timezone.utc).date().isoformat()
    out = str(bronze_dir / f"ingest_date={today}")
    df.write.mode("append").parquet(out)
    LOG.info("Bronze written: %s", out)

def main(since: str | None):
    setup_logging()
    cfg = load_yaml("config/sources.yaml")["census"]
    base_url = cfg["base_url"]
    dataset_path = cfg["dataset_path"]
    params = dict(cfg["params"])

    if since:
        params["time"] = f"from {since}"

    raw_dir = Path(cfg["output"]["raw_dir"])
    bronze_dir = Path(cfg["output"]["bronze_dir"])
    _ensure_dir(raw_dir)
    _ensure_dir(bronze_dir)

    rows = fetch_census_payload(base_url, dataset_path, params)
    records = normalize_tabular_json(rows)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw_path = raw_dir / f"census_{ts}.jsonl"
    write_jsonl(records, raw_path)
    LOG.info("Raw JSONL saved: %s (%d records)", raw_path, len(records))

    to_bronze_parquet(raw_path, bronze_dir)
    LOG.info("Done.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Census → bronze")
    parser.add_argument("--since", help='Overrides time window, e.g. "2023" or "2024-01"', default=None)
    args = parser.parse_args()
    main(args.since)