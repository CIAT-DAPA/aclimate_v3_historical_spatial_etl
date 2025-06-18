import json
from unittest.mock import patch, mock_open
from pathlib import Path
import pytest

from aclimate_v3_historical_spatial_etl.connectors import CopernicusDownloader


DUMMY_CONFIG = {
    "default_dataset": "test_dataset",
    "datasets": {
        "test_dataset": {
            "format": "nc",
            "variables": {
                "temperature": {
                    "name": "2m_temperature",
                    "output_dir": "temperature"
                }
            }
        }
    }
}


class TestCopernicusDownloaderInternal:

    @pytest.fixture
    def dummy_config_path(self, tmp_path):
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(DUMMY_CONFIG))
        return str(config_file)

    @pytest.fixture
    def downloader(self, tmp_path, dummy_config_path):
        with patch("cdsapi.Client"):
            return CopernicusDownloader(
                config_path=dummy_config_path,
                start_date="2024-01",
                end_date="2024-02",
                download_data_path=str(tmp_path),
                keep_nc_files=True
            )

    def test_load_config_valid(self, dummy_config_path):
        with patch("cdsapi.Client"), patch("builtins.open", mock_open(read_data=json.dumps(DUMMY_CONFIG))):
            downloader = CopernicusDownloader(
                config_path=dummy_config_path,
                start_date="2024-01",
                end_date="2024-01",
                download_data_path="dummy_path"
            )
            assert downloader.config["default_dataset"] == "test_dataset"

    def test_initialize_paths_creates_dirs(self, tmp_path, dummy_config_path):
        with patch("cdsapi.Client"):
            downloader = CopernicusDownloader(
                config_path=dummy_config_path,
                start_date="2024-01",
                end_date="2024-01",
                download_data_path=str(tmp_path)
            )
            expected_path = tmp_path / "temperature"
            assert expected_path.exists()
            assert expected_path.is_dir()

    def test_generate_days(self):
        d = CopernicusDownloader.__new__(CopernicusDownloader)
        days = d._generate_days(2024, 2)
        assert days == [f"{i:02d}" for i in range(1, 30)]  # 2024 is leap year

    def test_generate_month_range_within_same_year(self):
        d = CopernicusDownloader.__new__(CopernicusDownloader)
        result = d._generate_month_range(2024, 2024, 2, 2024, 4)
        assert result == ["02", "03", "04"]

    def test_generate_month_range_across_years(self):
        d = CopernicusDownloader.__new__(CopernicusDownloader)
        result = d._generate_month_range(2024, 2023, 11, 2025, 3)
        assert result == [f"{i:02d}" for i in range(1, 13)]

    def test_build_request_structure(self, downloader):
        dataset = "test_dataset"
        dataset_config = downloader.config["datasets"][dataset]
        var_config = dataset_config["variables"]["temperature"]

        request = downloader._build_request(
            dataset_name=dataset,
            dataset_config=dataset_config,
            var_config=var_config,
            year=2024,
            month="01",
            days=["01", "02"]
        )

        assert request["variable"] == ["2m_temperature"]
        assert request["year"] == ["2024"]
        assert request["month"] == ["01"]
        assert request["day"] == ["01", "02"]
        assert request["format"] == "nc"
