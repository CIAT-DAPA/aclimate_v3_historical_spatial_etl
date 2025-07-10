import os
import tempfile
import shutil
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from aclimate_v3_historical_spatial_etl.climate_processing import ClimatologyProcessor

# Test configuration data
DUMMY_NAMING_CONFIG = {
    "file_naming": {
        "template": "{temporal}_{country}_{variable}_{date}.tif",
        "components": {
            "variable_mapping": {
                "precipitation": "pr"
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

@pytest.fixture
def temp_output_dir():
    dir_path = tempfile.mkdtemp()
    yield Path(dir_path)
    shutil.rmtree(dir_path)

@patch.dict(os.environ, {
    "GEOSERVER_URL": "https://example.com/geoserver",
    "GEOSERVER_USER": "user",
    "GEOSERVER_PASSWORD": "pass"
})
def test_initialization_success(temp_output_dir):
    processor = ClimatologyProcessor(
        geoserver_workspace="workspace",
        geoserver_layer="layer",
        geoserver_store="store",
        output_path=temp_output_dir,
        variable="precipitation",
        naming_config=DUMMY_NAMING_CONFIG,
        countries_config=DUMMY_COUNTRIES_CONFIG
    )

    assert processor.template == "{temporal}_{country}_{variable}_{date}.tif"
    assert processor.country_code == "hn"
    assert processor.variable_mapping["precipitation"] == "pr"
    assert processor.variable == "precipitation"
    assert processor.output_path.exists()

def test_generate_climatology_name(temp_output_dir):
    with patch.dict(os.environ, {"GEOSERVER_URL": "http://test"}):
        processor = ClimatologyProcessor(
            geoserver_workspace="workspace",
            geoserver_layer="layer",
            geoserver_store="store",
            output_path=temp_output_dir,
            variable="precipitation",
            naming_config=DUMMY_NAMING_CONFIG,
            countries_config=DUMMY_COUNTRIES_CONFIG
        )
    result = processor._generate_climatology_name("01")
    assert result == "climatology_hn_pr_200001.tif"

def test_missing_env_vars(temp_output_dir):
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="Missing required environment variables"):
            ClimatologyProcessor(
                geoserver_workspace="workspace",
                geoserver_layer="layer",
                geoserver_store="store",
                output_path=temp_output_dir,
                variable="precipitation",
                naming_config=DUMMY_NAMING_CONFIG,
                countries_config=DUMMY_COUNTRIES_CONFIG
            )

def test_missing_country(temp_output_dir):
    invalid_countries_config = {
        "countries": {}  # No default country specified
    }
    
    with patch.dict(os.environ, {"GEOSERVER_URL": "http://test"}):
        with pytest.raises(ValueError, match="No country specified and no default country in config"):
            ClimatologyProcessor(
                geoserver_workspace="workspace",
                geoserver_layer="layer",
                geoserver_store="store",
                output_path=temp_output_dir,
                variable="precipitation",
                naming_config=DUMMY_NAMING_CONFIG,
                countries_config=invalid_countries_config
            )

def test_calculate_climatology(temp_output_dir):
    with patch.dict(os.environ, {"GEOSERVER_URL": "http://test"}):
        processor = ClimatologyProcessor(
            geoserver_workspace="workspace",
            geoserver_layer="layer",
            geoserver_store="store",
            output_path=temp_output_dir,
            variable="precipitation",
            naming_config=DUMMY_NAMING_CONFIG,
            countries_config=DUMMY_COUNTRIES_CONFIG
        )
        
        # Mock GeoServer interactions
        with patch.object(processor, 'get_dates_from_geoserver') as mock_dates, \
             patch.object(processor, '_download_from_geoserver') as mock_download, \
             patch('rioxarray.open_rasterio') as mock_open_rasterio, \
             patch('xarray.concat') as mock_concat:
            
            mock_dates.return_value = ["2020-01-01", "2020-01-02"]
            mock_download.return_value = temp_output_dir / "test.tif"
            mock_open_rasterio.return_value = MagicMock()
            mock_concat.return_value = MagicMock()
            
            results = processor.calculate_climatology()
            
            assert isinstance(results, dict)
            assert mock_dates.called
            assert mock_download.called

def test_cleanup(temp_output_dir):
    with patch.dict(os.environ, {"GEOSERVER_URL": "http://test"}):
        processor = ClimatologyProcessor(
            geoserver_workspace="workspace",
            geoserver_layer="layer",
            geoserver_store="store",
            output_path=temp_output_dir,
            variable="precipitation",
            naming_config=DUMMY_NAMING_CONFIG,
            countries_config=DUMMY_COUNTRIES_CONFIG
        )
        
        # Create a mock open dataset
        mock_ds = MagicMock()
        processor._open_datasets.append(mock_ds)
        
        processor.cleanup()
        
        # Verify cleanup actions
        mock_ds.close.assert_called_once()
        assert len(processor._open_datasets) == 0