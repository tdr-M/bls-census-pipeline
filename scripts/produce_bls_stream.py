from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests
from kafka import KafkaProducer

from jobs.utils.config_loader import load_yaml

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except Exception:
    pass

def _bls_payload(series: List[str], start_year: int | None, end_year: int | None) -> Dict[str, Any]:
    body: Dict[str, Any] = {"seriesid": series}
    if start_year:
        body["startyear"] = str(start_year)
    if end_year:
        body["endyear"] = str(end_year)
    key = os.getenv("BLS_API_KEY")
    if key:
        body["registrationkey"] = key
    return body

def fetch_bls(api_url: str, series: List[str], start_year: int | None, end_year: int | None) -> Dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    resp = requests.post(
        api_url,
        headers=headers,
        data=json.dumps(_bls_payload(series, start_year, end_year)),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()

def _latest_only(results: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for series in results.get("Results", {}).get("series", []):
        sid = series.get("seriesID")
        rows = series.get("data", [])
        pick = None
        for r in rows:
            if r.get("latest") in (True, "true", "True"):
                pick = r
                break
        if not pick and rows:
            pick = rows[0]
        if pick:
            out.append({"seriesID": sid, **pick})
    return out

def _all_points(results: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for series in results.get("Results", {}).get("series", []):
        sid = series.get("seriesID")
        for r in series.get("data", []):
            out.append({"seriesID": sid, **r})
    return out

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="config/streaming_source.yaml")
    p.add_argument("--kafka", default="config/kafka.yaml")
    p.add_argument("--latest-only", action="store_true", default=None)
    p.add_argument("--bootstrap", default=None)
    p.add_argument("--topic", default=None)
    args = p.parse_args()

    bls = load_yaml(args.config)["bls"]
    kfk = load_yaml(args.kafka)["kafka"]

    api_url = bls["api_url"]
    series = bls["series_ids"]
    start_year = bls.get("start_year")
    end_year = bls.get("end_year")
    latest_only = bls.get("latest_only") if args.latest_only is None else True

    bootstrap = args.bootstrap or os.getenv("KAFKA_BOOTSTRAP") or kfk.get("bootstrap_host")
    topic = args.topic or kfk["topic"]

    bls_key_present = "SET" if os.getenv("BLS_API_KEY") else "MISSING"
    print(f"[producer] bootstrap={bootstrap} topic={topic} bls_key={bls_key_present}")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda d: json.dumps(d).encode("utf-8"),
        key_serializer=lambda s: str(s).encode("utf-8"),
        linger_ms=20,
        acks="all",
    )

    iters = int(bls["poll"]["iterations"])
    interval = float(bls["poll"]["interval_seconds"])

    for i in range(iters):
        payload = fetch_bls(api_url, series, start_year, end_year)
        rows = _latest_only(payload) if latest_only else _all_points(payload)
        now_iso = datetime.now(timezone.utc).isoformat()

        sent = 0
        for r in rows:
            msg = {
                "ts_ingested": now_iso,
                "series_id": r.get("seriesID"),
                "year": r.get("year"),
                "period": r.get("period"),
                "periodName": r.get("periodName"),
                "value": r.get("value"),
                "footnotes": [f.get("text") for f in r.get("footnotes", []) if f],
                "is_latest": str(r.get("latest", "false")).lower() in ("true", "1"),
            }
            key = f"{msg['series_id']}|{msg['year']}|{msg['period']}"
            producer.send(topic, key=key, value=msg)
            sent += 1

        producer.flush()
        print(f"[produce_bls_stream] iter={i+1}/{iters} sent={sent} to topic={topic}")
        time.sleep(interval)

    producer.close()

if __name__ == "__main__":
    main()