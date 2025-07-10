from unittest.mock import patch, MagicMock
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
    def downloader(self, tmp_path):
        with patch("cdsapi.Client"):
            return CopernicusDownloader(
                config=DUMMY_CONFIG,
                start_date="2024-01",
                end_date="2024-02",
                download_data_path=str(tmp_path),
                keep_nc_files=True
            )

    def test_initialize_with_config_dict(self, tmp_path):
        with patch("cdsapi.Client"):
            downloader = CopernicusDownloader(
                config=DUMMY_CONFIG,
                start_date="2024-01",
                end_date="2024-01",
                download_data_path=str(tmp_path)
            )
            assert downloader.config["default_dataset"] == "test_dataset"

    def test_initialize_paths_creates_dirs(self, tmp_path):
        with patch("cdsapi.Client"):
            downloader = CopernicusDownloader(
                config=DUMMY_CONFIG,
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

    def test_netcdf_to_raster_conversion(self, tmp_path):
        with patch("cdsapi.Client"), \
             patch("xarray.open_dataset"), \
             patch("rioxarray.raster_array.RasterArray.to_raster"):
            
            downloader = CopernicusDownloader(
                config=DUMMY_CONFIG,
                start_date="2024-01",
                end_date="2024-01",
                download_data_path=str(tmp_path)
            )
            
            # Create dummy files for processing
            var_dir = tmp_path / "temperature" / "2024"
            var_dir.mkdir(parents=True)
            (var_dir / "temperature_20240101.nc").touch()
            
            downloader.netcdf_to_raster()
            
            # Verify the output directory was created
            assert (tmp_path / "temperature" / "2024").exists()