from io import StringIO

import boto3
import pandas as pd

from market_risk.ingestion.base import DataSource


class S3Source(DataSource):
    def __init__(self, bucket: str, prefix: str, region: str = "us-east-1"):
        self.bucket = bucket
        self.prefix = prefix
        self.client = boto3.client("s3", region_name=region)

    def list_files(self, prefix: str = "") -> list[str]:
        full_prefix = f"{self.prefix}{prefix}" if prefix else self.prefix
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=full_prefix)
        contents = response.get("Contents", [])
        return [obj["Key"] for obj in contents if obj["Key"].endswith(".csv")]

    def read_file(self, path: str) -> pd.DataFrame:
        response = self.client.get_object(Bucket=self.bucket, Key=path)
        body = response["Body"].read().decode("utf-8")
        return pd.read_csv(StringIO(body), parse_dates=["date"])
