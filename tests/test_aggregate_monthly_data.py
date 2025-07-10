import pytest
from pathlib import Path
from unittest.mock import patch
from aclimate_v3_historical_spatial_etl.climate_processing import MonthlyProcessor

DUMMY_NAMING_CONFIG = {
    "file_naming": {
        "template": "{temporal}_{country}_{variable}_{date}.tif",
        "components": {
            "variable_mapping": {
                "precipitation": "prec"
            }
        }
    }
}

DUMMY_COUNTRIES_CONFIG = {
    "default_country": "HONDURAS",
    "countries": {
        "HONDURAS": {
            "iso2_code": "HN"
        }
    }
}

class TestMonthlyProcessor:

    @pytest.fixture
    def processor(self, tmp_path):
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"

        return MonthlyProcessor(
            input_path=input_dir,
            output_path=output_dir,
            naming_config=DUMMY_NAMING_CONFIG,
            countries_config=DUMMY_COUNTRIES_CONFIG,
            country="HONDURAS"
        )

    def test_initialization_success(self, processor):
        assert processor.input_path.exists()
        assert processor.output_path.exists()
        assert processor.template == "{temporal}_{country}_{variable}_{date}.tif"
        assert processor.country_code == "hn"

    def test_generate_output_name(self, processor):
        filename = processor._generate_output_name("precipitation", "202301")
        assert filename == "monthly_hn_prec_202301.tif"

    def test_missing_input_path(self, tmp_path):
        invalid_input = tmp_path / "nonexistent"
        with pytest.raises(ValueError, match="Input path does not exist"):
            MonthlyProcessor(
                input_path=invalid_input,
                output_path=tmp_path / "out",
                naming_config=DUMMY_NAMING_CONFIG,
                countries_config=DUMMY_COUNTRIES_CONFIG,
                country="HONDURAS"
            )

    def test_missing_country_in_config(self, tmp_path):
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        with pytest.raises(ValueError, match="not found in configuration"):
            MonthlyProcessor(
                input_path=input_dir,
                output_path=tmp_path / "out",
                naming_config=DUMMY_NAMING_CONFIG,
                countries_config=DUMMY_COUNTRIES_CONFIG,
                country="BRAZIL"
            )

    def test_process_monthly_averages(self, tmp_path):
        # Create test input structure
        input_dir = tmp_path / "input" / "precipitation" / "2023"
        input_dir.mkdir(parents=True)
        (input_dir / "precipitation_20230101.tif").touch()
        (input_dir / "precipitation_20230102.tif").touch()

        processor = MonthlyProcessor(
            input_path=tmp_path / "input",
            output_path=tmp_path / "output",
            naming_config=DUMMY_NAMING_CONFIG,
            countries_config=DUMMY_COUNTRIES_CONFIG,
            country="HONDURAS"
        )

        with patch('rioxarray.open_rasterio') as mock_open_rasterio, \
             patch('xarray.concat') as mock_concat, \
             patch.object(MonthlyProcessor, '_process_month') as mock_process_month:
            
            mock_open_rasterio.return_value = 'mock_raster'
            processor.process_monthly_averages()
            
            assert mock_process_month.called
            assert (tmp_path / "output").exists()