import logging
import logging.config
from pathlib import Path
import yaml

def setup_logging(config_path: str | Path = "config/logging.yaml") -> None:
    p = Path(config_path)
    if p.exists():
        with p.open("r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        logging.config.dictConfig(cfg)
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )