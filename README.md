# AClimate V3 Historical Spatial ETL 🌍📊

## 🏷️ Version & Tags

![GitHub release (latest by date)](https://img.shields.io/github/v/release/CIAT-DAPA/aclimate_v3_historical_spatial_etl) ![](https://img.shields.io/github/v/tag/CIAT-DAPA/aclimate_v3_historical_spatial_etl)

**Tags:** `climate-data`, `etl`, `geoprocessing`, `python`, `geoserver`, `chirps`, `copernicus`, `AGera5`

---

## 📌 Introduction

Python package for processing spatial historical climate data with a complete ETL pipeline that includes:

- Data download from CHIRPS and Copernicus sources
- Spatial clipping by country boundaries
- Monthly aggregation and climatology calculations
- GeoServer integration for data publishing

**Key Features:**

- Automated processing of temperature, precipitation, and solar radiation data
- Flexible configuration for multiple countries and variables
- End-to-end pipeline from raw data to published layers

---

## ✅ Prerequisites

- Python > 3.10
- **Copernicus Climate Data Store (CDS) API key** - [Register here](https://cds.climate.copernicus.eu/)
- GeoServer

## ⚙️ Installation

```bash
pip install git+https://github.com/CIAT-DAPA/aclimate_v3_historical_spatial_etl
```

To install a specific version:

```bash
pip install git+https://github.com/CIAT-DAPA/aclimate_v3_historical_spatial_etl@v0.0.1
```

## 🚀 Basic Usage

1. Command Line Interface

```bash
python -m aclimate_v3_historical_spatial_etl.aclimate_run_etl \
  --country HONDURAS \
  --start_date 2020-01 \
  --end_date 2020-12 \
  --data_path /path/to/data \
  --climatology

```

> [!NOTE]  
>  Options:
>
> - `--skip_download`: Skip the data download step
> - `--climatology`: Calculate monthly averages-climatology
> - `--no_cleanup`: Keep intermediate files after processing

2. Programmatic Usage

```python
from aclimate_v3_historical_spatial_etl.aclimate_run_etl import run_etl_pipeline

run_etl_pipeline(
    country="HONDURAS",
    start_date="2020-01",
    end_date="2020-12",
    data_path="/path/to/data",
    climatology=True
)

```

## 🗂️ Directory Structure (Auto-generated)

```bash
data/
├── config/               # Must contain required JSON config files
├── raw_data/             # Downloaded raw datasets
├── process_data/         # Intermediate raster data
├── calc_data/
│   ├── climatology_data/ # Climatology outputs
│   └── monthly_data/     # Monthly processed rasters
└── upload_geoserver/     # Output prepared for GeoServer publishing

```

## 🔧 Configuration

### Required Config Files

Place these in your `config` directory:

1. `chirps_config.json` - CHIRPS download settings
2. `copernicus_config.json` - Copernicus/ERA5 settings
3. `clipping_config.json` - Country boundaries and ISO codes
4. `geoserver_config.json` - GeoServer workspace and store names
5. `naming_config.json` - Output file naming conventions

### Environment Variables

- Windows:

```bash
# GeoServer credentials
set GEOSERVER_URL=http://localhost:8086/geoserver/rest/
set GEOSERVER_USER=admin
set GEOSERVER_PASSWORD=password
set OTLP_ENDPOINT=localhost:4317
```

- Linux/Ubuntu:

```bash
# GeoServer credentials
export GEOSERVER_URL=http://localhost:8086/geoserver/rest/
export GEOSERVER_USER=admin
export GEOSERVER_PASSWORD=password
export OTLP_ENDPOINT=localhost:4317
```

> [!NOTE]  
>  Options:
>
> - `GEOSERVER_URL`: Geoserver URL
> - `GEOSERVER_USER`: Geoserver user
> - `GEOSERVER_PASSWORD`: Geoserver password
> - `OTLP_ENDPOINT`: Signoz endpoint to send logs

## 🧪 Running Tests

```bash
# Install test requirements
pip install pytest pytest-mock

# Run tests
pytest tests/
```

## 🔄 CI/CD Pipeline Overview

### Workflow Architecture

Our GitHub Actions pipeline implements a three-stage deployment process:

```bash
Code Push → Test Stage → Merge Stage → Release Stage
```

## 📊 Project Structure

```bash
aclimate_v3_historical_spatial_etl/
│
├── .github/
│ └── workflows/ # CI/CD pipeline configurations
├── src/
│   └── aclimate_v3_historical_spatial_etl/
│       ├── connectors/           # Downloaders: CHIRPS, Copernicus
│       ├── tools/                # Clipping and GeoServer tools
│       ├── climate_processing/   # Monthly and climatology processors
│       ├── config/               # Example config files
│       └── aclimate_run_etl.py   # Main ETL entry script
├── tests/                       # Unit and integration tests
├── requirements.txt             # Dependencies
└── setup.py                     # Packaging
```
