
from market_risk.ingestion.local_source import LocalSource


class TestLocalSource:
    def test_list_files(self, tmp_path):
        (tmp_path / "data1.csv").write_text("ticker,date,open,high,low,close,volume\n")
        (tmp_path / "data2.csv").write_text("ticker,date,open,high,low,close,volume\n")
        (tmp_path / "readme.txt").write_text("ignore me")

        source = LocalSource(str(tmp_path))
        files = source.list_files()
        assert len(files) == 2
        assert all(f.endswith(".csv") for f in files)

    def test_list_files_empty_directory(self, tmp_path):
        source = LocalSource(str(tmp_path))
        assert source.list_files() == []

    def test_list_files_nonexistent_path(self, tmp_path):
        source = LocalSource(str(tmp_path / "nonexistent"))
        assert source.list_files() == []

    def test_read_file(self, tmp_path):
        csv_content = (
            "ticker,date,open,high,low,close,volume\n"
            "AAPL,2024-01-02,185.5,186.2,184.8,185.9,50000000\n"
        )
        (tmp_path / "test.csv").write_text(csv_content)

        source = LocalSource(str(tmp_path))
        df = source.read_file("test.csv")
        assert len(df) == 1
        assert df.iloc[0]["ticker"] == "AAPL"
        assert df.iloc[0]["close"] == 185.9
