from pathlib import Path

import pandas as pd

from market_risk.ingestion.base import DataSource


class LocalSource(DataSource):
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)

    def list_files(self, prefix: str = "") -> list[str]:
        search_path = self.base_path / prefix if prefix else self.base_path
        if not search_path.exists():
            return []
        return [str(p.relative_to(self.base_path)) for p in search_path.glob("*.csv")]

    def read_file(self, path: str) -> pd.DataFrame:
        full_path = self.base_path / path
        return pd.read_csv(full_path, parse_dates=["date"])
