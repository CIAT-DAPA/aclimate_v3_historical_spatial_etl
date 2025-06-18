
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import shutil
import sys
import json
from .connectors import CopernicusDownloader, ChirpsDownloader
from .tools import RasterClipper, GeoServerUploadPreparer, logging_manager, error, info, warning
from .climate_processing import MonthlyProcessor, ClimatologyProcessor


class ETLError(Exception):
    """Custom exception for ETL pipeline errors"""
    pass
#python -m src.aclimate_v3_historical_spatial_etl.aclimate_run_etl --country HONDURAS --start_date 2025-04 --end_date 2025-04 --data_path "D:\\Code\\aclimate_v3_historical_spatial_etl\\data_test"
def parse_args():
    """Parse simplified command line arguments."""
    info("Parsing command line arguments", component="setup")
    parser = argparse.ArgumentParser(description="Climate Data ETL Pipeline")
    
    # Required arguments
    parser.add_argument("--country", required=True, help="Country name for processing")
    parser.add_argument("--start_date", required=True, help="Start date in YYYY-MM format")
    parser.add_argument("--end_date", required=True, help="End date in YYYY-MM format")
    parser.add_argument("--data_path", required=True, help="Base directory for all data")
    
    # Pipeline control flags
    parser.add_argument("--skip_download", action="store_true", help="Skip data download step")
    parser.add_argument("--climatology", action="store_true", help="Calculate climatology")
    parser.add_argument("--no_cleanup", action="store_true", help="Disable automatic cleanup")
    
    args = parser.parse_args()
    info("Command line arguments parsed successfully", 
         component="setup",
         args=vars(args))
    return args

def validate_dates(start_date: str, end_date: str):
    """Validate date format and range."""
    try:
        info("Validating date range", 
             component="validation",
             start_date=start_date,
             end_date=end_date)
        
        start = datetime.strptime(start_date, "%Y-%m")
        end = datetime.strptime(end_date, "%Y-%m")
        if start > end:
            raise ValueError("Start date must be before end date")
            
        info("Date validation successful", component="validation")
    except ValueError as e:
        error("Invalid date format", 
              component="validation",
              error=str(e))
        raise ETLError(f"Invalid date format. Use YYYY-MM. Error: {str(e)}")

def setup_directory_structure(base_path: Path) -> Dict[str, Path]:
    """Create and validate the required directory structure."""
    info("Setting up directory structure", 
         component="setup",
         base_path=str(base_path))
    
    paths = {
        'config': base_path / "config",
        'raw_data': base_path / "raw_data",
        'processed_data': base_path / "process_data",
        'calc_data': base_path / "calc_data",
        'climatology_data': base_path / "calc_data" / "climatology_data",
        'monthly_data': base_path / "calc_data" / "monthly_data",
        'upload_geoserver': base_path / "upload_geoserver"
    }
    
    # Validate config directory exists with required files
    required_config_files = [
        "chirps_config.json",
        "clipping_config.json",
        "copernicus_config.json",
        "naming_config.json",
        "geoserver_config.json"
    ]
    
    if not paths['config'].exists():
        error("Config directory not found", 
              component="setup",
              path=str(paths['config']))
        raise ETLError(f"Config directory not found: {paths['config']}")
    
    missing_files = []
    for file in required_config_files:
        if not (paths['config'] / file).exists():
            missing_files.append(file)
    
    if missing_files:
        error("Missing config files",
              component="setup",
              missing_files=missing_files)
        raise ETLError(f"Missing config files: {', '.join(missing_files)}")
    
    # Create other directories if they don't exist
    for key, path in paths.items():
        if key != 'config':
            try:
                path.mkdir(parents=True, exist_ok=True)
                info(f"Directory created/verified", 
                     component="setup",
                     path=str(path))
            except Exception as e:
                error("Failed to create directory",
                      component="setup",
                      path=str(path),
                      error=str(e))
                raise ETLError(f"Could not create directory {path}: {str(e)}")
    
    return paths

def load_config_with_iso2(config_path: Path, country: str) -> tuple:
    """Load both geoserver and clipping configs and extract ISO2 code."""
    try:
        info("Loading configuration files", 
             component="config",
             country=country)
        
        # Load clipping config to get ISO2 code
        with open(config_path / "clipping_config.json") as f:
            clipping_config = json.load(f)
            
            # Get ISO2 code for the country
            country_data = clipping_config["countries"].get(country.upper())
            if not country_data:
                error("Country not found in config",
                      component="config",
                      country=country)
                raise ETLError(f"Country '{country}' not found in clipping_config.json")
            
            iso2 = country_data.get("iso2_code")
            if not iso2:
                error("ISO2 code missing for country",
                      component="config",
                      country=country)
                raise ETLError(f"No ISO2 code found for country '{country}'")
        
        # Load geoserver config
        with open(config_path / "geoserver_config.json") as f:
            geoserver_config = json.load(f)
            
            # Process store names to replace [iso2] with actual code
            for data_type in geoserver_config.values():
                if "stores" in data_type:
                    for var_name, store_name in data_type["stores"].items():
                        if "[iso2]" in store_name:
                            data_type["stores"][var_name] = store_name.replace("[iso2]", iso2)
            
            info("Configuration loaded successfully",
                 component="config",
                 iso2_code=iso2)
            return geoserver_config, iso2
            
    except json.JSONDecodeError as e:
        error("Invalid JSON in config file",
              component="config",
              error=str(e))
        raise ETLError(f"Invalid JSON in config file: {str(e)}")
    except Exception as e:
        error("Failed to load config files",
              component="config",
              error=str(e))
        raise ETLError(f"Could not read config files: {str(e)}")

def get_variables_from_config(config_path: Path) -> List[str]:
    """Extract variables from naming config file."""
    try:
        info("Extracting variables from config",
             component="config")
        
        with open(config_path / "naming_config.json") as f:
            config = json.load(f)
            variable_mapping = config["file_naming"]["components"]["variable_mapping"]
            variables = list(variable_mapping.keys())
            
            info("Variables extracted successfully",
                 component="config",
                 variables=variables)
            return variables
    except Exception as e:
        error("Failed to extract variables from config",
              component="config",
              error=str(e))
        raise ETLError(f"Could not read variables from config: {str(e)}")

def clean_directory(path: Path, force: bool = False):
    """Clean directory contents with safety checks."""
    if not path.exists():
        warning("Directory does not exist - skipping cleanup",
               component="cleanup",
               path=str(path))
        return
    
    if not force:
        response = input(f"Are you sure you want to clean {path}? [y/N]: ")
        if response.lower() != 'y':
            info("Cleanup cancelled by user",
                 component="cleanup",
                 path=str(path))
            return
    
    try:
        items_deleted = 0
        for item in path.glob("*"):
            if item.is_file():
                item.unlink()
                items_deleted += 1
            elif item.is_dir():
                shutil.rmtree(item)
                items_deleted += 1
                
        info("Directory cleanup completed",
             component="cleanup",
             path=str(path),
             items_deleted=items_deleted)
    except Exception as e:
        error("Failed to clean directory",
              component="cleanup",
              path=str(path),
              error=str(e))
        raise

def run_etl_pipeline(args):
    """Execute the enhanced ETL pipeline with dynamic store naming."""
    try:
        info("Starting ETL pipeline", component="main")
        
        # Validate inputs
        validate_dates(args.start_date, args.end_date)
        
        # Setup directory structure
        base_path = Path(args.data_path)
        paths = setup_directory_structure(base_path)
        
        # Load configurations with ISO2 code substitution
        geoserver_config, iso2 = load_config_with_iso2(paths['config'], args.country)
        variables = get_variables_from_config(paths['config'])
        info("Configuration loaded",
             component="main",
             variables=variables,
             iso2_code=iso2)
        
        # Initialize downloaders
        copernicus_downloader = None
        chirps_downloader = None
        
        # Step 1: Data Download
        if not args.skip_download:
            info("Starting data download phase", component="download")
            
            copernicus_downloader = CopernicusDownloader(
                config_path=paths['config'] / "copernicus_config.json",
                start_date=args.start_date,
                end_date=args.end_date,
                download_data_path=paths['raw_data']
            )
            copernicus_downloader.main()
            
            chirps_downloader = ChirpsDownloader(
                config_path=paths['config'] / "chirps_config.json",
                start_date=args.start_date,
                end_date=args.end_date,
                download_data_path=paths['raw_data']
            )
            chirps_downloader.main()
            
            info("Data download completed", component="download")
        
        # Step 2: Clipping Data
        info("Starting data clipping phase", component="clipping")
        clipper = RasterClipper(
            country=args.country,
            downloader_configs={
                'copernicus': paths['config'] / "copernicus_config.json",
                'chirps': paths['config'] / "chirps_config.json"
            },
            naming_config_path=paths['config'] / "naming_config.json",
            clipping_config_path=paths['config'] / "clipping_config.json"
        )
        clipper.process_all(
            base_download_path=paths['raw_data'],
            base_processed_path=paths['processed_data']
        )
        info("Data clipping completed", component="clipping")
        
        # Step 3: Upload Processed Data to GeoServer
        info("Starting GeoServer upload for raw data", component="geoserver")
        preparer = GeoServerUploadPreparer(
            source_data_path=paths['processed_data'],
            upload_base_path=paths['upload_geoserver']
        )
        
        raw_config = geoserver_config['raw_data']
        for variable in variables:
            info(f"Processing variable for GeoServer upload", 
                 component="geoserver",
                 variable=variable)
            
            upload_dir = preparer.prepare_for_upload(variable)
            
            store_name = raw_config['stores'].get(variable)
            if not store_name:
                error("No store name configured for variable",
                      component="geoserver",
                      variable=variable)
                raise ETLError(f"No store name configured for variable {variable} in raw_data")
            
            preparer.upload_to_geoserver(
                workspace=raw_config['workspace'],
                store=store_name,
                date_format="yyyyMMdd"
            )
            clean_directory(paths['upload_geoserver'], True)
        
        info("Raw data GeoServer upload completed", component="geoserver")
        
        # Step 4: Monthly Processing and Upload
        if args.climatology:
            info("Starting monthly processing", component="processing")
            monthly_processor = MonthlyProcessor(
                input_path=paths['processed_data'],
                output_path=paths['monthly_data'],
                naming_config_path=paths['config'] / "naming_config.json",
                countries_config_path=paths['config'] / "clipping_config.json",
                country=args.country
            )
            monthly_processor.process_monthly_averages()
            
            info("Starting monthly data GeoServer upload", component="geoserver")
            monthly_preparer = GeoServerUploadPreparer(
                source_data_path=paths['monthly_data'],
                upload_base_path=paths['upload_geoserver']
            )
            
            monthly_config = geoserver_config['monthly_data']
            for variable in variables:
                info(f"Processing monthly variable for GeoServer upload",
                     component="geoserver",
                     variable=variable)
                
                upload_dir = monthly_preparer.prepare_for_upload(f"{variable}")
                
                store_name = monthly_config['stores'].get(variable)
                if not store_name:
                    error("No store name configured for monthly variable",
                          component="geoserver",
                          variable=variable)
                    raise ETLError(f"No store name configured for variable {variable} in monthly_data")
                
                monthly_preparer.upload_to_geoserver(
                    workspace=monthly_config['workspace'],
                    store=store_name,
                    date_format="yyyyMM"
                )
                clean_directory(paths['upload_geoserver'], True)
            
            info("Monthly processing and upload completed", component="processing")
            
            # Step 5: Climatology Calculation and Upload
            info("Starting climatology calculation", component="processing")
            monthly_config = geoserver_config['monthly_data']
            for variable in variables:
                info(f"Calculating climatology for variable",
                     component="processing",
                     variable=variable)
                
                store_name = monthly_config['stores'].get(variable)
                if not store_name:
                    error("No store name configured for climatology variable",
                          component="processing",
                          variable=variable)
                    raise ETLError(f"No store name configured for variable {variable} in climatology_data")

                climatology_processor = ClimatologyProcessor(
                    output_path=paths['climatology_data'],
                    naming_config_path=paths['config'] / "naming_config.json",
                    countries_config_path=paths['config'] / "clipping_config.json",
                    country=args.country,
                    geoserver_workspace=monthly_config['workspace'],
                    geoserver_layer=f"{monthly_config['workspace']}:{store_name}",
                    geoserver_store=store_name,
                    variable=variable
                )
                climatology_processor.calculate_climatology()
            
            info("Starting climatology data GeoServer upload", component="geoserver")
            clim_preparer = GeoServerUploadPreparer(
                source_data_path=paths['climatology_data'],
                upload_base_path=paths['upload_geoserver']
            )
            
            clim_config = geoserver_config['climatology_data']
            for variable in variables:
                info(f"Processing climatology variable for GeoServer upload",
                     component="geoserver",
                     variable=variable)
                    
                upload_dir = clim_preparer.prepare_for_upload(f"{variable}")
                
                store_name = clim_config['stores'].get(variable)
                if not store_name:
                    error("No store name configured for climatology variable",
                          component="geoserver",
                          variable=variable)
                    raise ETLError(f"No store name configured for variable {variable} in climatology_data")
                
                clim_preparer.upload_to_geoserver(
                    workspace=clim_config['workspace'],
                    store=store_name,
                    date_format="yyyyMM"
                )
                clean_directory(paths['upload_geoserver'], True)
            
            info("Climatology processing and upload completed", component="processing")
        
        # Step 6: Cleanup
        if not args.no_cleanup:
            info("Starting cleanup phase", component="cleanup")
            
            if copernicus_downloader:
                copernicus_downloader.clean_rasters()
            
            clean_directory(paths['raw_data'], True)
            clean_directory(paths['processed_data'], True)
            clean_directory(paths['monthly_data'], True)
            clean_directory(paths['climatology_data'], True)
            
            info("Cleanup completed", component="cleanup")
        
        info("ETL pipeline completed successfully", component="main")
    
    except ETLError as e:
        error("ETL pipeline failed",
              component="main",
              error=str(e))
        sys.exit(1)
    except Exception as e:
        error("Unexpected error in ETL pipeline",
              component="main",
              error=str(e))
        sys.exit(1)

def main():
    args = parse_args()
    run_etl_pipeline(args)

if __name__ == "__main__":
    main()