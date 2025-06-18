import os
import json
import tempfile
import shutil
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from aclimate_v3_historical_spatial_etl.climate_processing import ClimatologyProcessor

@pytest.fixture
def dummy_config_files():
    tmp_dir = tempfile.mkdtemp()
    naming_path = Path(tmp_dir) / "naming.json"
    countries_path = Path(tmp_dir) / "countries.json"

    naming_data = {
        "file_naming": {
            "template": "{temporal}_{country}_{variable}_{date}.tif",
            "components": {
                "variable_mapping": {
                    "precipitation": "pr"
                }
            }
        }
    }

    countries_data = {
        "default_country": "HONDURAS",
        "countries": {
            "HONDURAS": {
                "iso2_code": "HN"
            }
        }
    }

    with open(naming_path, "w") as f:
        json.dump(naming_data, f)

    with open(countries_path, "w") as f:
        json.dump(countries_data, f)

    yield naming_path, countries_path

    shutil.rmtree(tmp_dir)

@patch.dict(os.environ, {
    "GEOSERVER_URL": "https://example.com/geoserver",
    "GEOSERVER_USER": "user",
    "GEOSERVER_PASSWORD": "pass"
})
def test_initialization_success(dummy_config_files):
    naming_path, countries_path = dummy_config_files
    processor = ClimatologyProcessor(
        geoserver_workspace="workspace",
        geoserver_layer="layer",
        geoserver_store="store",
        output_path=tempfile.mkdtemp(),
        variable="precipitation",
        naming_config_path=naming_path,
        countries_config_path=countries_path
    )

    assert processor.template == "{temporal}_{country}_{variable}_{date}.tif"
    assert processor.country_code == "hn"
    assert processor.variable_mapping["precipitation"] == "pr"
    assert processor.variable == "precipitation"


def test_generate_climatology_name(dummy_config_files):
    naming_path, countries_path = dummy_config_files
    with patch.dict(os.environ, {"GEOSERVER_URL": "http://test"}):
        processor = ClimatologyProcessor(
            geoserver_workspace="workspace",
            geoserver_layer="layer",
            geoserver_store="store",
            output_path=tempfile.mkdtemp(),
            variable="precipitation",
            naming_config_path=naming_path,
            countries_config_path=countries_path
        )
    result = processor._generate_climatology_name("01")
    assert result == "climatology_hn_pr_200001.tif"


def test_missing_env_vars(dummy_config_files):
    naming_path, countries_path = dummy_config_files

    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="Missing required environment variables"):
            ClimatologyProcessor(
                geoserver_workspace="workspace",
                geoserver_layer="layer",
                geoserver_store="store",
                output_path=tempfile.mkdtemp(),
                variable="precipitation",
                naming_config_path=naming_path,
                countries_config_path=countries_path
            )


def test_missing_country(dummy_config_files):
    naming_path, countries_path = dummy_config_files

    with patch.dict(os.environ, {"GEOSERVER_URL": "http://test"}):
        with open(countries_path, "w") as f:
            json.dump({"countries": {}}, f)

        with pytest.raises(ValueError, match="No country specified and no default country in config"):
            ClimatologyProcessor(
                geoserver_workspace="workspace",
                geoserver_layer="layer",
                geoserver_store="store",
                output_path=tempfile.mkdtemp(),
                variable="precipitation",
                naming_config_path=naming_path,
                countries_config_path=countries_path
            )
