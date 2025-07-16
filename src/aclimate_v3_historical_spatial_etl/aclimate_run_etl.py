
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Union, Any
import shutil
import sys
import json
from .connectors import CopernicusDownloader, ChirpsDownloader
from .tools import RasterClipper, GeoServerUploadPreparer, logging_manager, error, info, warning
from .climate_processing import MonthlyProcessor, ClimatologyProcessor
from aclimate_v3_orm.services import MngDataSourceService

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

def setup_directory_structure(base_path: Path) -> Dict[str, Union[Dict[str, Any], Path]]:
    """Create directory structure and load configurations using DataSourceService."""
    info("Setting up directory structure and loading configurations",
         component="setup",
         base_path=str(base_path))

    # 1. Setup directory paths (sin el directorio config)
    paths = {
        'raw_data': base_path / "raw_data",
        'processed_data': base_path / "process_data",
        'calc_data': base_path / "calc_data",
        'climatology_data': base_path / "calc_data" / "climatology_data",
        'monthly_data': base_path / "calc_data" / "monthly_data",
        'upload_geoserver': base_path / "upload_geoserver"
    }

    # 2. Configuraciones requeridas
    required_configs = {
        "chirps_config": None,
        "clipping_config": None,
        "copernicus_config": None,
        "naming_config": None,
        "geoserver_config": None
    }

    # 3. Obtener configuraciones usando el servicio
    data_source_service = MngDataSourceService()
    loaded_configs = {}
    missing_configs = []

    for config_name in required_configs.keys():
        try:
            # Buscar en la base de datos usando el servicio
            db_config = data_source_service.get_by_name(name=f"{config_name}")
            
            if not db_config or not db_config.content:
                missing_configs.append(config_name)
                continue

            # Parsear el contenido JSON
            config_content = json.loads(db_config.content)
            loaded_configs[config_name] = config_content
            info(f"Config loaded successfully",
                 component="setup",
                 config_name=config_name)

        except json.JSONDecodeError as e:
            error("Invalid JSON in configuration",
                  component="setup",
                  config_name=config_name,
                  error=str(e))
            missing_configs.append(config_name)
        except Exception as e:
            error("Failed to load configuration",
                  component="setup",
                  config_name=config_name,
                  error=str(e))
            missing_configs.append(config_name)

    if missing_configs:
        error("Missing or invalid configurations",
              component="setup",
              missing_configs=missing_configs)
        raise ETLError(f"Missing or invalid configs: {', '.join(missing_configs)}")

    for path in paths.values():
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

    return {
        'paths': paths,
        'configs': loaded_configs
    }


def load_config_with_iso2(configs: Dict[str, Any], country: str) -> tuple:
    """Load both geoserver and clipping configs and extract ISO2 code."""
    try:
        info("Processing configuration from database", 
             component="config",
             country=country)
        
        # Get clipping config from loaded configs
        clipping_config = configs["clipping_config"]
        if not clipping_config:
            error("Clipping config not found in loaded configurations",
                  component="config")
            raise ETLError("Clipping configuration not found in database")
            
        # Get ISO2 code for the country
        country_data = clipping_config["countries"].get(country.upper())
        if not country_data:
            error("Country not found in config",
                  component="config",
                  country=country)
            raise ETLError(f"Country '{country}' not found in clipping config")
        
        iso2 = country_data.get("iso2_code")
        if not iso2:
            error("ISO2 code missing for country",
                  component="config",
                  country=country)
            raise ETLError(f"No ISO2 code found for country '{country}'")
        
        # Get geoserver config
        geoserver_config = configs.get("geoserver_config")
        if not geoserver_config:
            error("Geoserver config not found in loaded configurations",
                  component="config")
            raise ETLError("Geoserver configuration not found in database")
        
        # Process store names to replace [iso2] with actual code
        for data_type in geoserver_config.values():
            if "stores" in data_type:
                for var_name, store_name in data_type["stores"].items():
                    if "[iso2]" in store_name:
                        data_type["stores"][var_name] = store_name.replace("[iso2]", iso2)
        
        info("Configuration processed successfully",
             component="config",
             iso2_code=iso2)
        return geoserver_config, iso2
            
    except KeyError as e:
        error("Missing required key in configuration",
              component="config",
              error=str(e))
        raise ETLError(f"Missing key in configuration: {str(e)}")
    except Exception as e:
        error("Failed to process configs",
              component="config",
              error=str(e))
        raise ETLError(f"Could not process configurations: {str(e)}")

def get_variables_from_config(configs: Dict[str, Any]) -> List[str]:
    """Extract variables from naming config."""
    try:
        info("Extracting variables from naming config",
             component="config")
        
        naming_config = configs["naming_config"]
        if not naming_config:
            error("Naming config not found in loaded configurations",
                  component="config")
            raise ETLError("Naming configuration not found in database")
        
        variable_mapping = naming_config["file_naming"]["components"]["variable_mapping"]
        variables = list(variable_mapping.keys())
        
        info("Variables extracted successfully",
             component="config",
             variables=variables)
        return variables
        
    except KeyError as e:
        error("Missing required key in naming config",
              component="config",
              error=str(e))
        raise ETLError(f"Missing key in naming configuration: {str(e)}")
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
        setup_result = setup_directory_structure(base_path)
        configs = setup_result['configs']
        paths = setup_result['paths']

        # Load configurations with ISO2 code substitution
        geoserver_config, iso2 = load_config_with_iso2(configs, args.country)
        variables = get_variables_from_config(configs)
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
                config=configs["copernicus_config"],
                start_date=args.start_date,
                end_date=args.end_date,
                download_data_path=paths['raw_data']
            )
            copernicus_downloader.main()
            
            chirps_downloader = ChirpsDownloader(
                config=configs["chirps_config"],
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
                'copernicus': configs["copernicus_config"],
                'chirps': configs["chirps_config"]
            },
            naming_config=configs["naming_config"],
            clipping_config=configs["clipping_config"]
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
        info("Starting monthly processing", component="processing")
        monthly_processor = MonthlyProcessor(
            input_path=paths['processed_data'],
            output_path=paths['monthly_data'],
            naming_config=configs["naming_config"],
            countries_config=configs["clipping_config"],
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
        if args.climatology:
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
                    naming_config=configs["naming_config"],
                    countries_config=configs["clipping_config"],
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