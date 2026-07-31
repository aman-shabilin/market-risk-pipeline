from abc import ABC, abstractmethod

import pandas as pd


class DataSource(ABC):
    @abstractmethod
    def list_files(self, prefix: str = "") -> list[str]:
        ...

    @abstractmethod
    def read_file(self, path: str) -> pd.DataFrame:
        ...
