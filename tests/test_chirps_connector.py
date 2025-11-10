from unittest.mock import patch, MagicMock
from pathlib import Path
import pytest

from aclimate_v3_historical_spatial_etl.connectors import ChirpsDownloader


DUMMY_CONFIG = {
    "datasets": {
        "CHIRPS": {
            "output_dir": "Precipitation",
            "url_config": {
                "base_url": "https://fake.chirps.data/",
                "file_pattern": "chirps-v2.0.date.tif.gz"
            },
            "file_naming": "{variable}_{date}.tif.gz"
        }
    },
    "parallel_downloads": 4
}


class TestChirpsDownloaderInternal:

    @pytest.fixture
    def downloader(self, tmp_path):
        with patch("aclimate_v3_historical_spatial_etl.tools.Tools") as MockTools:
            MockTools.return_value.generate_dates.return_value = ["2023-01-01", "2023-01-02"]
            instance = ChirpsDownloader(
                config=DUMMY_CONFIG,
                start_date="2023-01",
                end_date="2023-01",
                download_data_path=str(tmp_path)
            )
            return instance

    def test_initialize_with_config_dict(self, tmp_path):
        with patch("aclimate_v3_historical_spatial_etl.tools.Tools"):
            downloader = ChirpsDownloader(
                config=DUMMY_CONFIG,
                start_date="2023-01",
                end_date="2023-01",
                download_data_path=str(tmp_path)
            )
            assert "CHIRPS" in downloader.config["datasets"]

    def test_initialize_paths_creates_chirps_dir(self, tmp_path):
        with patch("aclimate_v3_historical_spatial_etl.tools.Tools"):
            downloader = ChirpsDownloader(
                config=DUMMY_CONFIG,
                start_date="2023-01",
                end_date="2023-01",
                download_data_path=str(tmp_path)
            )
            expected_path = tmp_path / "Precipitation"
            assert expected_path.exists() and expected_path.is_dir()

    def test_build_download_url(self, downloader):
        url = downloader._build_download_url("2023-01-01")
        assert url == "https://fake.chirps.data/2023/chirps-v2.0.2023.01.01.tif.gz"

    def test_download_data_builds_paths(self, tmp_path):
        with patch("aclimate_v3_historical_spatial_etl.tools.Tools") as MockTools:
            MockTools.return_value.generate_dates.return_value = ["2023-01", "2023-01"]

            with patch("aclimate_v3_historical_spatial_etl.connectors.chirps_connector.ChirpsDownloader._download_file"):
                downloader = ChirpsDownloader(
                    config=DUMMY_CONFIG,
                    start_date="2023-01",
                    end_date="2023-01",
                    download_data_path=str(tmp_path)
                )
                result_paths = downloader.download_data()

        assert len(result_paths) == 31, f"Expected 31 paths, got {len(result_paths)}"
        assert all(p.suffix == ".tif" for p in result_paths)

    def test_download_file_handles_existing_file(self, tmp_path, downloader):
        test_file = tmp_path / "test.tif.gz"
        test_file.touch()
        uncompressed = tmp_path / "test.tif"
        uncompressed.touch()

        with patch("urllib.request.urlretrieve"), \
             patch("gzip.open"), \
             patch("builtins.open"):
            
            downloader._download_file("http://test.url", test_file)
            # Should skip download since file exists

    def test_parallel_downloads_from_config(self, tmp_path):
        with patch("aclimate_v3_historical_spatial_etl.tools.Tools"):
            downloader = ChirpsDownloader(
                config=DUMMY_CONFIG,
                start_date="2023-01",
                end_date="2023-01",
                download_data_path=str(tmp_path)
            )
            assert downloader.cores == 4