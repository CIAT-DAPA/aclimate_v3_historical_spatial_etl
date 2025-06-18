import json
from unittest.mock import patch, mock_open
from pathlib import Path
import pytest

from aclimate_v3_historical_spatial_etl.connectors import ChirpsDownloader


DUMMY_CONFIG = {
    "datasets": {
        "chirps": {
            "output_dir": "Precipitation",
            "url_config": {
                "base_url": "https://fake.chirps.data/",
                "file_pattern": "chirps-v2.0.date.tif.gz"
            },
            "file_naming": "{variable}_{date}.tif.gz"
        }
    },
    "parallel_downloads": 2
}


class TestChirpsDownloaderInternal:

    @pytest.fixture
    def dummy_config_path(self, tmp_path):
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(DUMMY_CONFIG))
        return str(config_file)

    @pytest.fixture
    def downloader(self, tmp_path, dummy_config_path):
        with patch("aclimate_v3_historical_spatial_etl.tools.Tools") as MockTools:
            MockTools.return_value.generate_dates.return_value = ["2023-01-01", "2023-01-02"]
            instance = ChirpsDownloader(
                config_path=dummy_config_path,
                start_date="2023-01-01",
                end_date="2023-01-02",
                download_data_path=str(tmp_path)
            )
            return instance

    def test_load_config_valid(self, dummy_config_path):
        with patch("builtins.open", mock_open(read_data=json.dumps(DUMMY_CONFIG))):
            downloader = ChirpsDownloader(
                config_path=dummy_config_path,
                start_date="2023-01-01",
                end_date="2023-01-02",
                download_data_path="dummy_path"
            )
            assert "chirps" in downloader.config["datasets"]

    def test_initialize_paths_creates_chirps_dir(self, tmp_path, dummy_config_path):
        with patch("aclimate_v3_historical_spatial_etl.tools.Tools"):
            downloader = ChirpsDownloader(
                config_path=dummy_config_path,
                start_date="2023-01-01",
                end_date="2023-01-02",
                download_data_path=str(tmp_path)
            )
            expected_path = tmp_path / "Precipitation"
            assert expected_path.exists() and expected_path.is_dir()

    def test_build_download_url(self, downloader):
        url = downloader._build_download_url("2023-01-01")
        assert url == "https://fake.chirps.data/2023/chirps-v2.0.2023.01.01.tif.gz"

    def test_download_data_builds_paths(self, tmp_path, dummy_config_path):
        with patch("aclimate_v3_historical_spatial_etl.connectors.chirps_connector.Tools") as MockTools:
            MockTools.return_value.generate_dates.return_value = ["2023-01-01", "2023-01-02"]

            with patch("aclimate_v3_historical_spatial_etl.connectors.chirps_connector.ChirpsDownloader._download_file"):
                downloader = ChirpsDownloader(
                    config_path=dummy_config_path,
                    start_date="2023-01-01",
                    end_date="2023-01-02",
                    download_data_path=str(tmp_path)
                )
                result_paths = downloader.download_data()

        assert len(result_paths) == 2, f"Expected 2 paths, got {len(result_paths)}"
        assert all(p.suffix == ".tif" for p in result_paths)


