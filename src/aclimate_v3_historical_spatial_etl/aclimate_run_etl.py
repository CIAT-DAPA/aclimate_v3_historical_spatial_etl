
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Union, Any, Tuple
import shutil
import sys
import json
from .connectors import CopernicusDownloader, ChirpsDownloader
from .tools import RasterClipper, GeoServerUploadPreparer, logging_manager, error, info, warning
from .climate_processing import MonthlyProcessor, ClimatologyProcessor, IndicatorsProcessor
from aclimate_v3_orm.services import MngDataSourceService
from aclimate_v3_orm.database.base import create_tables

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
    parser.add_argument("--start_date", help="Start date in YYYY-MM format (required unless using --skip_processing with --indicators)")
    parser.add_argument("--end_date", help="End date in YYYY-MM format (required unless using --skip_processing with --indicators)")
    parser.add_argument("--data_path", required=True, help="Base directory for all data")
    # Pipeline control flags
    parser.add_argument("--skip_download", action="store_true", help="Skip data download step")
    parser.add_argument("--skip_processing", action="store_true", help="Skip all data processing steps (download, clipping, monthly aggregation, climatology) - useful for indicators-only runs")
    parser.add_argument("--climatology", action="store_true", help="Calculate climatology")
    parser.add_argument("--indicators", action="store_true", help="Calculate climate indicators")
    parser.add_argument("--indicator_years", type=str, help="Year range for indicator calculation (e.g., '2020-2023')")
    parser.add_argument("--no_cleanup", action="store_true", help="Disable automatic cleanup")
    parser.add_argument("--init", action="store_true", help="Initialize database tables before running ETL")
    
    args = parser.parse_args()
    
    # Custom validation for start_date and end_date
    indicators_only = args.skip_processing and args.indicators
    
    if not indicators_only:
        # For regular processing, start_date and end_date are required
        if not args.start_date:
            parser.error("--start_date is required when not using --skip_processing with --indicators")
        if not args.end_date:
            parser.error("--end_date is required when not using --skip_processing with --indicators")
    
    # For indicators-only mode, indicator_years is required
    if args.indicators and not args.indicator_years:
        parser.error("--indicator_years is required when using --indicators")
    
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
            raise ETLError("Start date must be before end date")
            
        info("Date validation successful", component="validation")
    except ValueError as e:
        error("Invalid date format", 
              component="validation",
              error=str(e))
        raise ETLError(f"Invalid date format. Use YYYY-MM. Error: {str(e)}")

def validate_indicator_years(indicator_years: str) -> Tuple[str, str]:
    """
    Validate and parse indicator year range.
    
    Args:
        indicator_years: Year range string in format 'YYYY-YYYY' or single year 'YYYY'
        
    Returns:
        Tuple of (start_year, end_year)
    """
    try:
        if not indicator_years:
            raise ValueError("Indicator years range is required")
        
        # Handle single year format
        if '-' not in indicator_years:
            try:
                year = int(indicator_years)
                if year < 1900 or year > 2030:
                    raise ValueError("Year must be between 1900 and 2030")
                
                info("Single year indicator calculation",
                     component="validation",
                     year=year)
                
                return str(year), str(year)
            except ValueError as e:
                if "must be between" in str(e):
                    raise e
                raise ValueError("Invalid year format. Use 'YYYY' or 'YYYY-YYYY' format")
        
        # Handle year range format
        start_year_str, end_year_str = indicator_years.split('-', 1)
        
        # Validate year format
        start_year = int(start_year_str)
        end_year = int(end_year_str)
        
        if start_year > end_year:
            raise ValueError("Start year must be before or equal to end year")
        
        if start_year < 1900 or end_year > 2030:
            raise ValueError("Years must be between 1900 and 2030")
        
        info("Indicator years validation successful",
             component="validation",
             start_year=start_year,
             end_year=end_year)
        
        return str(start_year), str(end_year)
        
    except ValueError as e:
        error("Invalid indicator years format",
              component="validation",
              indicator_years=indicator_years,
              error=str(e))
        raise ETLError(f"Invalid indicator years format. Use 'YYYY' or 'YYYY-YYYY'. Error: {str(e)}")

def setup_directory_structure(base_path: Path, country_name: str) -> Dict[str, Union[Dict[str, Any], Path]]:
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
        'indicators_data': base_path / "calc_data" / "indicators_data",
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
            db_config = data_source_service.get_by_name_and_country(name=f"{config_name}", country_name=country_name)
            
            if not db_config or not db_config.content:
                missing_configs.append(config_name)
                continue

            # Parsear el contenido JSON
            config_content = json.loads(db_config.content)
            loaded_configs[config_name] = config_content
            info(f"Config loaded successfully {config_name}",
                 component="setup",
                 config_name=config_name)

        except json.JSONDecodeError as e:
            error(f"Invalid JSON in configuration {config_name}",
                  component="setup",
                  config_name=config_name,
                  error=str(e))
            missing_configs.append(config_name)
        except Exception as e:
            error(f"Failed to load configuration {config_name}",
                  component="setup",
                  config_name=config_name,
                  error=str(e))
            missing_configs.append(config_name)

    if missing_configs:
        error(f"Missing or invalid configurations: {', '.join(missing_configs)}",
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
        # Check if running in interactive mode
        if sys.stdin.isatty():
            response = input(f"Are you sure you want to clean {path}? [y/N]: ")
            if response.lower() != 'y':
                info("Cleanup cancelled by user",
                     component="cleanup",
                     path=str(path))
                return
        else:
            warning("Non-interactive mode detected - skipping cleanup confirmation",
                   component="cleanup",
                   path=str(path))
            return
    
    try:
        items_deleted = 0
        # Convert to list to avoid iterator issues during deletion
        items_to_delete = list(path.glob("*"))
        
        for item in items_to_delete:
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
        error(f"Failed to clean directory {str(path)}",
              component="cleanup",
              path=str(path),
              error=str(e))
        raise ETLError(f"Failed to clean directory {str(path)}: {str(e)}")

def run_etl_pipeline(args):
    """Execute the enhanced ETL pipeline with dynamic store naming."""
    try:
        info("Starting ETL pipeline", component="main")

        if getattr(args, "init", False):
            info("Initializing database tables via create_tables()", component="main")
            create_tables()
            info("Database tables created successfully", component="main")
        
        # Validate inputs
        if args.start_date and args.end_date:
            validate_dates(args.start_date, args.end_date)
        
        # Validate indicator years if provided
        indicator_start_year, indicator_end_year = None, None
        if args.indicators and args.indicator_years:
            indicator_start_year, indicator_end_year = validate_indicator_years(args.indicator_years)
            info("Indicator years validated",
                 component="validation",
                 indicator_years=f"{indicator_start_year}-{indicator_end_year}")
        
        # Setup directory structure
        base_path = Path(args.data_path)
        setup_result = setup_directory_structure(base_path, args.country)
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
        if not args.skip_download and not args.skip_processing:
            if not args.start_date or not args.end_date:
                error("start_date and end_date are required for data download", component="download")
                raise ETLError("start_date and end_date are required when downloading data")
                
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
        else:
            info("Skipping data download phase (skip_processing enabled)", component="download")

        
        # Step 2: Clipping Data
        if not args.skip_processing:
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
        else:
            info("Skipping data clipping phase (skip_processing enabled)", component="clipping")

        #Step 3: Upload Processed Data to GeoServer
        if not args.skip_processing:
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
                    error(f"No store name configured for variable {variable}",
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
        else:
            info("Skipping GeoServer upload for raw data (skip_processing enabled)", component="geoserver")
        
        # Step 4: Monthly Processing and Upload
        if not args.skip_processing:
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
        else:
            info("Skipping monthly processing and upload (skip_processing enabled)", component="processing")
            
            # Step 5: Climatology Calculation and Upload
        if args.climatology and not args.skip_processing:
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
        
        # Step 6: Indicators Calculation
        if args.indicators:
            info("Starting indicators calculation", component="processing")
            
            # Use indicator-specific years if provided, otherwise use data processing dates or error
            if indicator_start_year and indicator_end_year:
                indicator_start_date = f"{indicator_start_year}-01"
                indicator_end_date = f"{indicator_end_year}-12"
            elif args.start_date and args.end_date:
                indicator_start_date = args.start_date
                indicator_end_date = args.end_date
            else:
                # Error out if no dates are provided for indicators
                error("No dates provided for indicators calculation. Use --indicator_years or provide --start_date and --end_date",
                      component="processing")
                raise ETLError("Indicators calculation requires date range. Use --indicator_years 'YYYY-YYYY' or provide --start_date and --end_date arguments.")
            
            info("Indicators date range determined",
                 component="processing",
                 indicator_start_date=indicator_start_date,
                 indicator_end_date=indicator_end_date)
            
            indicators_processor = IndicatorsProcessor(
                country=args.country,
                start_date=indicator_start_date,
                end_date=indicator_end_date,
                output_path=paths['indicators_data'],
                naming_config=configs["naming_config"],
                countries_config=configs["clipping_config"]
            )
            
            # Process all indicators for the country
            indicators_processor.process_all_indicators()
            
            # Get list of processed indicators for logging
            available_indicators = indicators_processor.get_available_indicators()
            indicator_names = [ind.get('short_name', 'Unknown') for ind in available_indicators]
            
            info(f"Indicators calculation completed {indicator_names}", 
                 component="processing",
                 country=args.country,
                 indicators_count=len(available_indicators),
                 indicators=indicator_names)
            
            # Step 6.5: Upload Indicators Data to GeoServer
            info("Starting GeoServer upload for indicators data", component="geoserver")
            indicators_preparer = GeoServerUploadPreparer(
                source_data_path=paths['indicators_data'],
                upload_base_path=paths['upload_geoserver']
            )
            indicators_config = geoserver_config.get('indicators_data')
            if not indicators_config:
                warning("No indicators_data config found in geoserver_config - using fallback configuration",
                       component="geoserver")
                # Fallback configuration if not present in config
                indicators_config = {
                    'workspace': f'climate_index',
                    'stores': {}
                }
                # Generate store names for available indicators
                for indicator in available_indicators:
                    short_name = indicator.get('short_name', 'unknown')
                    indicators_config['stores'][short_name] = f'climate_index_{iso2}_{short_name}'
            # Process each calculated indicator
            for indicator in available_indicators:
                indicator_short_name = indicator.get('short_name', 'unknown')
                info(f"Processing indicator for GeoServer upload", 
                     component="geoserver",
                     indicator=indicator_short_name)
                upload_dir = indicators_preparer.prepare_for_upload(indicator_short_name)
                
                store_name = indicators_config['stores'].get(indicator_short_name)
                if not store_name:
                    # Generate fallback store name if not configured
                    store_name = f'climate_index_{iso2}_{indicator_short_name}'
                    warning(f"No store name configured for indicator {indicator_short_name}, using fallback",
                           component="geoserver",
                           indicator=indicator_short_name,
                           fallback_store=store_name)
                
                indicators_preparer.upload_to_geoserver(
                    workspace=indicators_config['workspace'],
                    store=store_name,
                    date_format="yyyy"
                )
                clean_directory(paths['upload_geoserver'], True)
            
            info("Indicators data GeoServer upload completed", component="geoserver")
        
        # Step 7: Cleanup
        if not args.no_cleanup:
            info("Starting cleanup phase", component="cleanup")
            
            clean_directory(paths['raw_data'], True)
            clean_directory(paths['processed_data'], True)
            clean_directory(paths['monthly_data'], True)
            clean_directory(paths['climatology_data'], True)
            clean_directory(paths['indicators_data'], True)
            
            info("Cleanup completed", component="cleanup")
        
        info("ETL pipeline completed successfully", component="main")
    
    except ETLError as e:
        error(f"ETL pipeline failed {str(e)}",
              component="main",
              error=str(e))
        sys.exit(1)
    except Exception as e:
        error(f"Unexpected error in ETL pipeline {str(e)}",
              component="main",
              error=str(e))
        sys.exit(1)

def main():
    args = parse_args()
    run_etl_pipeline(args)

if __name__ == "__main__":
    main()