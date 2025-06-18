import json
import pytest
from pathlib import Path
from unittest.mock import patch, mock_open
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
    def config_paths(self, tmp_path):
        naming_config = tmp_path / "naming.json"
        countries_config = tmp_path / "countries.json"

        naming_config.write_text(json.dumps(DUMMY_NAMING_CONFIG))
        countries_config.write_text(json.dumps(DUMMY_COUNTRIES_CONFIG))

        return naming_config, countries_config

    @pytest.fixture
    def processor(self, tmp_path, config_paths):
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"
        naming_config, countries_config = config_paths

        return MonthlyProcessor(
            input_path=input_dir,
            output_path=output_dir,
            naming_config_path=naming_config,
            countries_config_path=countries_config,
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

    def test_missing_input_path(self, tmp_path, config_paths):
        invalid_input = tmp_path / "nonexistent"
        naming_config, countries_config = config_paths
        with pytest.raises(ValueError, match="Input path does not exist"):
            MonthlyProcessor(
                input_path=invalid_input,
                output_path=tmp_path / "out",
                naming_config_path=naming_config,
                countries_config_path=countries_config,
                country="HONDURAS"
            )

    def test_missing_country_in_config(self, tmp_path, config_paths):
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        naming_config, countries_config = config_paths
        with pytest.raises(ValueError, match="not found in configuration"):
            MonthlyProcessor(
                input_path=input_dir,
                output_path=tmp_path / "out",
                naming_config_path=naming_config,
                countries_config_path=countries_config,
                country="BRAZIL"
            )
