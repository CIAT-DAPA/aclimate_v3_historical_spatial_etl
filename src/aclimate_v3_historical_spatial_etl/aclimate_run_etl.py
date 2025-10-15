
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
from aclimate_v3_orm.database.base import create_tables

class ETLError(Exception):
    """Custom exception for ETL pipeline errors"""
    pass
#python -m src.aclimate_v3_historical_spatial_etl.aclimate_run_etl --countries HONDURAS,GUATEMALA,PANAMA --start_date 2025-04 --end_date 2025-04 --data_path "D:\\Code\\aclimate_v3_historical_spatial_etl\\data_test"
def parse_args():
    """Parse simplified command line arguments."""
    info("Parsing command line arguments", component="setup")
    parser = argparse.ArgumentParser(description="Climate Data ETL Pipeline")
    
    # Required arguments
    parser.add_argument("--countries", required=True, help="Country names for processing (comma-separated list, e.g., HONDURAS,GUATEMALA,PANAMA)")
    parser.add_argument("--start_date", required=True, help="Start date in YYYY-MM format")
    parser.add_argument("--end_date", required=True, help="End date in YYYY-MM format")
    parser.add_argument("--data_path", required=True, help="Base directory for all data")
    
    # Pipeline control flags
    parser.add_argument("--skip_download", action="store_true", help="Skip data download step")
    parser.add_argument("--climatology", action="store_true", help="Calculate climatology")
    parser.add_argument("--no_cleanup", action="store_true", help="Disable automatic cleanup")
    parser.add_argument("--init", action="store_true", help="Initialize database tables before running ETL")
    
    args = parser.parse_args()
    
    # Parse comma-separated countries
    if hasattr(args, 'countries') and args.countries:
        args.countries = [country.strip().upper() for country in args.countries.split(',') if country.strip()]
        info("Countries parsed from comma-separated string",
             component="setup",
             countries=args.countries)
    
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

def setup_directory_structure(base_path: Path, countries: List[str]) -> Dict[str, Union[Dict[str, Any], Path]]:
    """Create directory structure and load configurations using DataSourceService."""
    info("Setting up directory structure and loading configurations",
         component="setup",
         base_path=str(base_path),
         countries=countries)

    # 1. Setup global directory paths (shared data)
    global_paths = {
        'raw_data': base_path / "raw_data",  # Global downloads
        'upload_geoserver': base_path / "upload_geoserver"
    }
    
    # 2. Setup country-specific paths
    country_paths = {}
    for country in countries:
        country_upper = country.upper()
        country_paths[country_upper] = {
            'processed_data': base_path / "process_data" / country_upper,
            'calc_data': base_path / "calc_data" / country_upper,
            'climatology_data': base_path / "calc_data" / country_upper / "climatology_data",
            'monthly_data': base_path / "calc_data" / country_upper / "monthly_data"
        }

    # 3. Load global configurations (common to all countries)
    required_global_configs = {
        "chirps_config": None,
        "clipping_config": None,
        "copernicus_config": None,
        "geoserver_config": None
    }

    data_source_service = MngDataSourceService()
    loaded_configs = {}
    missing_configs = []

    # Load global configurations
    for config_name in required_global_configs.keys():
        try:
            db_config = data_source_service.get_by_name(name=f"{config_name}")
            
            if not db_config or not db_config.content:
                missing_configs.append(config_name)
                continue

            config_content = json.loads(db_config.content)
            loaded_configs[config_name] = config_content
            info(f"Global config loaded successfully {config_name}",
                 component="setup",
                 config_name=config_name)

        except json.JSONDecodeError as e:
            error(f"Invalid JSON in global configuration {config_name}",
                  component="setup",
                  config_name=config_name,
                  error=str(e))
            missing_configs.append(config_name)
        except Exception as e:
            error(f"Failed to load global configuration {config_name}",
                  component="setup",
                  config_name=config_name,
                  error=str(e))
            missing_configs.append(config_name)

    if missing_configs:
        error(f"Missing or invalid global configurations: {', '.join(missing_configs)}",
              component="setup",
              missing_configs=missing_configs)
        raise ETLError(f"Missing or invalid global configs: {', '.join(missing_configs)}")

    # Create all directories
    all_paths = {**global_paths}
    for country_path_dict in country_paths.values():
        all_paths.update(country_path_dict)
    
    for path in all_paths.values():
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
        'global_paths': global_paths,
        'country_paths': country_paths,
        'configs': loaded_configs
    }


def get_copernicus_variables_from_country_configs(copernicus_config, countries):
    """
    Extract Copernicus variables from country-specific configuration structure.
    This works with the new structure where copernicus_config has country sections.
    """
    consolidated_variables = set()
    variables_by_country = {}
    
    for country in countries:
        country_variables = []
        if country in copernicus_config:
            datasets = copernicus_config[country].get('datasets', {})
            for dataset_name, dataset_config in datasets.items():
                # Extract variables from the dataset config  
                variables = list(dataset_config.get('variables', {}).keys())
                country_variables.extend(variables)
                consolidated_variables.update(variables)
        
        variables_by_country[country] = country_variables
    
    return {
        'consolidated': list(consolidated_variables),
        'by_country': variables_by_country
    }


def modify_copernicus_config_for_global_download(copernicus_config, countries, variables):
    """
    Create a modified Copernicus config for global download with all variables.
    This handles the new country-specific structure.
    """
    # Create a consolidated config for global download
    modified_config = {"datasets": {}}
    
    # Collect all datasets from all countries
    all_datasets = {}
    for country in countries:
        if country in copernicus_config:
            country_datasets = copernicus_config[country].get('datasets', {})
            for dataset_name, dataset_config in country_datasets.items():
                if dataset_name not in all_datasets:
                    # Copy the dataset config and update variables
                    all_datasets[dataset_name] = dataset_config.copy()
                    # Create variables dict with all consolidated variables
                    consolidated_vars = {}
                    for var in variables:
                        # Try to find the variable config from any country
                        for c in countries:
                            if (c in copernicus_config and 
                                dataset_name in copernicus_config[c].get('datasets', {}) and
                                var in copernicus_config[c]['datasets'][dataset_name].get('variables', {})):
                                consolidated_vars[var] = copernicus_config[c]['datasets'][dataset_name]['variables'][var]
                                break
                    all_datasets[dataset_name]['variables'] = consolidated_vars
    
    modified_config["datasets"] = all_datasets
    return modified_config


def get_country_specific_copernicus_config(copernicus_config, country):
    """
    Extract country-specific Copernicus configuration.
    
    Args:
        copernicus_config: Dict with structure {COUNTRY: {datasets: {...}}}
        country: Country name (ISO2 code)
        
    Returns:
        Country-specific config or empty dict if not found
    """
    if country in copernicus_config:
        return copernicus_config[country]
    return {"datasets": {}}


def modify_config_for_variables(config, variables):
    """
    Modify configuration to include only specified variables.
    
    Args:
        config: Configuration dictionary with datasets structure
        variables: List of variable names to include
        
    Returns:
        Modified configuration with filtered variables
    """
    modified_config = config.copy()
    
    if 'datasets' in modified_config:
        for dataset_name, dataset_config in modified_config['datasets'].items():
            if 'variables' in dataset_config:
                # Filter variables to only include those in the specified list
                filtered_variables = [
                    var for var in dataset_config['variables'] 
                    if var in variables
                ]
                modified_config['datasets'][dataset_name]['variables'] = filtered_variables
    
    return modified_config


def load_configs_for_countries(countries):
    """
    Load configurations using the original system: one global config for most, only copernicus and chirps can change.
    
    Args:
        countries: List of country names (ISO2 codes)
        
    Returns:
        Dict with global configs and variables from naming config
    """
    info("Loading configurations using original system",
         component="config",
         countries=countries)
    
    data_source_service = MngDataSourceService()
    
    # Load the SINGLE global naming config (used by all countries)
    try:
        db_config = data_source_service.get_by_name(name="naming_config")
        
        if not db_config or not db_config.content:
            error("Global naming config not found",
                  component="config")
            raise ETLError("Global naming configuration not found")
        
        naming_config = json.loads(db_config.content)
        
        # Extract variables from the single naming config
        variable_mapping = naming_config["file_naming"]["components"]["variable_mapping"]
        all_variables = list(variable_mapping.keys())
        
        info("Global naming config loaded successfully",
             component="config",
             variables=all_variables)
             
    except json.JSONDecodeError as e:
        error("Invalid JSON in global naming configuration",
              component="config",
              error=str(e))
        raise ETLError("Invalid JSON in global naming configuration")
    except Exception as e:
        error("Failed to load global naming configuration",
              component="config",
              error=str(e))
        raise ETLError("Could not load global naming configuration")
    
    info("Global naming configuration loaded",
         component="config",
         total_countries=len(countries),
         variables=all_variables)
    
    return {
        'naming_config': naming_config,
        'all_variables': all_variables
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

def get_variables_from_country_configs(country_configs_data: Dict[str, Any]) -> Dict[str, List[str]]:
    """Extract variables from country configs and return both consolidated and per-country variables."""
    try:
        info("Extracting variables from country configs",
             component="config")
        
        all_variables = set()
        country_variables = {}
        
        for country, config_data in country_configs_data['country_configs'].items():
            naming_config = config_data['naming_config']
            variable_mapping = naming_config["file_naming"]["components"]["variable_mapping"]
            variables = list(variable_mapping.keys())
            
            country_variables[country] = variables
            all_variables.update(variables)
            
            info(f"Variables extracted for {country}",
                 component="config",
                 country=country,
                 variables=variables)
        
        consolidated_variables = list(all_variables)
        
        info("All variables extracted successfully",
             component="config",
             consolidated_variables=consolidated_variables,
             country_specific=country_variables)
        
        return {
            'consolidated': consolidated_variables,
            'by_country': country_variables
        }
        
    except KeyError as e:
        error("Missing required key in country configs",
              component="config",
              error=str(e))
        raise ETLError(f"Missing key in country configurations: {str(e)}")
    except Exception as e:
        error("Failed to extract variables from country configs",
              component="config",
              error=str(e))
        raise ETLError(f"Could not read variables from country configs: {str(e)}")

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

def process_country(country: str, 
                   country_config: Dict[str, Any],
                   country_variables: List[str],
                   global_configs: Dict[str, Any],
                   global_paths: Dict[str, Path],
                   country_paths: Dict[str, Path],
                   args) -> None:
    """Process a single country with its specific configuration and variables."""
    
    info(f"Starting processing for country {country}",
         component="country_processing",
         country=country,
         variables=country_variables)
    
    try:
        # Get country-specific configurations
        geoserver_config, iso2 = load_config_with_iso2_for_country(
            global_configs, country_config, country
        )
        
        # Step 1: Clipping Data for this country
        info(f"Starting data clipping for {country}",
             component="clipping",
             country=country)
        
        # Determine which copernicus config to use
        copernicus_config = global_configs["copernicus_config"]
        
        # Check if this is the new country-specific structure
        if country.upper() in copernicus_config:
            # Use country-specific copernicus config
            country_copernicus_config = get_country_specific_copernicus_config(
                copernicus_config, 
                country.upper()
            )
            info(f"Using country-specific Copernicus config for {country}",
                 component="clipping",
                 country=country)
        else:
            # Use global copernicus config (traditional structure)
            country_copernicus_config = copernicus_config
            info(f"Using global Copernicus config for {country}",
                 component="clipping",
                 country=country)
        
        clipper = RasterClipper(
            country=country,
            downloader_configs={
                'copernicus': country_copernicus_config,
                'chirps': global_configs["chirps_config"]
            },
            naming_config=country_config["naming_config"],
            clipping_config=global_configs["clipping_config"]
        )
        clipper.process_all(
            base_download_path=global_paths['raw_data'],
            base_processed_path=country_paths['processed_data']
        )
        info(f"Data clipping completed for {country}", 
             component="clipping",
             country=country)
        
        # Step 2: Upload Processed Data to GeoServer for this country
        info(f"Starting GeoServer upload for raw data - {country}",
             component="geoserver",
             country=country)
        
        preparer = GeoServerUploadPreparer(
            source_data_path=country_paths['processed_data'],
            upload_base_path=global_paths['upload_geoserver']
        )
        
        raw_config = geoserver_config['raw_data']
        for variable in country_variables:
            info(f"Processing variable {variable} for GeoServer upload - {country}",
                 component="geoserver",
                 country=country,
                 variable=variable)
            
            upload_dir = preparer.prepare_for_upload(variable)
            
            store_name = raw_config['stores'].get(variable)
            if not store_name:
                warning(f"Store not found for variable {variable} in country {country}",
                       component="geoserver",
                       country=country,
                       variable=variable)
                continue
            
            preparer.upload_to_geoserver(
                workspace=raw_config['workspace'],
                store=store_name,
                date_format="yyyyMMdd"
            )
            clean_directory(global_paths['upload_geoserver'], True)
        
        info(f"Raw data GeoServer upload completed for {country}",
             component="geoserver",
             country=country)
        
        # Step 3: Monthly Processing and Upload for this country
        info(f"Starting monthly processing for {country}",
             component="processing",
             country=country)
        
        monthly_processor = MonthlyProcessor(
            input_path=country_paths['processed_data'],
            output_path=country_paths['monthly_data'],
            naming_config=country_config["naming_config"],
            countries_config=global_configs["clipping_config"],
            country=country
        )
        monthly_processor.process_monthly_averages()
        
        info(f"Starting monthly data GeoServer upload for {country}",
             component="geoserver",
             country=country)
        
        monthly_preparer = GeoServerUploadPreparer(
            source_data_path=country_paths['monthly_data'],
            upload_base_path=global_paths['upload_geoserver']
        )
        
        monthly_config = geoserver_config['monthly_data']
        for variable in country_variables:
            info(f"Processing monthly variable {variable} for GeoServer upload - {country}",
                 component="geoserver",
                 country=country,
                 variable=variable)
            
            upload_dir = monthly_preparer.prepare_for_upload(f"{variable}")
            
            store_name = monthly_config['stores'].get(variable)
            if not store_name:
                warning(f"Monthly store not found for variable {variable} in country {country}",
                       component="geoserver",
                       country=country,
                       variable=variable)
                continue
            
            monthly_preparer.upload_to_geoserver(
                workspace=monthly_config['workspace'],
                store=store_name,
                date_format="yyyyMM"
            )
            clean_directory(global_paths['upload_geoserver'], True)
        
        info(f"Monthly processing and upload completed for {country}",
             component="processing",
             country=country)
        
        # Step 4: Climatology Calculation and Upload for this country
        if args.climatology:
            info(f"Starting climatology calculation for {country}",
                 component="processing",
                 country=country)
            
            climatology_processor = ClimatologyProcessor(
                input_path=country_paths['monthly_data'],
                output_path=country_paths['climatology_data'],
                naming_config=country_config["naming_config"],
                countries_config=global_configs["clipping_config"],
                country=country
            )
            climatology_processor.process_climatologies()
            
            info(f"Starting climatology data GeoServer upload for {country}",
                 component="geoserver",
                 country=country)
            
            clim_preparer = GeoServerUploadPreparer(
                source_data_path=country_paths['climatology_data'],
                upload_base_path=global_paths['upload_geoserver']
            )
            
            clim_config = geoserver_config['climatology_data']
            for variable in country_variables:
                info(f"Processing climatology variable {variable} for GeoServer upload - {country}",
                     component="geoserver",
                     country=country,
                     variable=variable)
                
                upload_dir = clim_preparer.prepare_for_upload(f"{variable}")
                
                store_name = clim_config['stores'].get(variable)
                if not store_name:
                    warning(f"Climatology store not found for variable {variable} in country {country}",
                           component="geoserver",
                           country=country,
                           variable=variable)
                    continue
                
                clim_preparer.upload_to_geoserver(
                    workspace=clim_config['workspace'],
                    store=store_name,
                    date_format="MM"
                )
                clean_directory(global_paths['upload_geoserver'], True)
            
            info(f"Climatology processing and upload completed for {country}",
                 component="processing",
                 country=country)
        
        # Step 5: Country-specific cleanup
        if not args.no_cleanup:
            info(f"Starting cleanup for {country}",
                 component="cleanup",
                 country=country)
            
            clean_directory(country_paths['processed_data'], True)
            clean_directory(country_paths['monthly_data'], True)
            clean_directory(country_paths['climatology_data'], True)
            
            info(f"Cleanup completed for {country}",
                 component="cleanup",
                 country=country)
        
        info(f"Country processing completed successfully for {country}",
             component="country_processing",
             country=country)
    
    except Exception as e:
        error(f"Failed to process country {country}",
              component="country_processing",
              country=country,
              error=str(e))
        raise ETLError(f"Failed to process country {country}: {str(e)}")


def load_config_with_iso2_for_country(global_configs: Dict[str, Any], 
                                     country_config: Dict[str, Any], 
                                     country: str) -> tuple:
    """Load geoserver config for a specific country and extract ISO2 code using global configs."""
    try:
        info(f"Processing geoserver configuration for {country}",
             component="config",
             country=country)
        
        # Get clipping config from global configs (it's global, not country-specific)
        clipping_config = global_configs["clipping_config"]
        if not clipping_config:
            error("Clipping config not found in global configurations",
                  component="config",
                  country=country)
            raise ETLError("Clipping configuration not found in database")
            
        # Get ISO2 code for the country from global clipping config
        country_data = clipping_config["countries"].get(country.upper())
        if not country_data:
            error(f"Country {country} not found in clipping config",
                  component="config",
                  country=country)
            raise ETLError(f"Country '{country}' not found in clipping config")
        
        iso2 = country_data.get("iso2_code")
        if not iso2:
            error(f"ISO2 code missing for country {country}",
                  component="config",
                  country=country)
            raise ETLError(f"No ISO2 code found for country '{country}'")
        
        # Get geoserver config from global configs (it's global, not country-specific)
        geoserver_config = global_configs.get("geoserver_config")
        if not geoserver_config:
            error("Geoserver config not found in global configurations",
                  component="config",
                  country=country)
            raise ETLError("Geoserver configuration not found in database")
        
        # Process store names to replace [iso2] with actual code (deep copy to avoid modifying original)
        processed_geoserver_config = json.loads(json.dumps(geoserver_config))
        for data_type in processed_geoserver_config.values():
            if "stores" in data_type:
                for variable, store_name in data_type["stores"].items():
                    if "[iso2]" in store_name:
                        data_type["stores"][variable] = store_name.replace("[iso2]", iso2)
        
        info(f"Geoserver configuration processed successfully for {country}",
             component="config",
             country=country,
             iso2_code=iso2)
        return processed_geoserver_config, iso2
            
    except KeyError as e:
        error(f"Missing required key in configuration for {country}",
              component="config",
              country=country,
              error=str(e))
        raise ETLError(f"Missing key in configuration for {country}: {str(e)}")
    except Exception as e:
        error(f"Failed to process geoserver config for {country}",
              component="config",
              country=country,
              error=str(e))
        raise ETLError(f"Could not process geoserver configuration for {country}: {str(e)}")

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
    """Execute the enhanced ETL pipeline with global download and multi-country processing."""
    try:
        info("Starting multi-country ETL pipeline", 
             component="main",
             countries=args.countries)

        if getattr(args, "init", False):
            info("Initializing database tables via create_tables()", component="main")
            create_tables()
            info("Database tables created successfully", component="main")
        
        # Validate inputs
        validate_dates(args.start_date, args.end_date)
        
        # Setup directory structure for multiple countries
        base_path = Path(args.data_path)
        setup_result = setup_directory_structure(base_path, args.countries)
        global_configs_from_setup = setup_result['configs']
        global_paths = setup_result['global_paths']
        country_paths = setup_result['country_paths']

        # Load configurations for all countries
        country_configs_data = load_configs_for_countries(args.countries)
        
        # Extract naming config and variables (global for all countries)
        global_naming_config = country_configs_data['naming_config']
        consolidated_variables = country_configs_data['all_variables']
        
        # Combine global configurations from setup with naming config
        global_configs = global_configs_from_setup
        global_configs['naming_config'] = global_naming_config
        
        # All countries use the same variables from the global naming config
        country_specific_variables = {}
        for country in args.countries:
            country_specific_variables[country.upper()] = consolidated_variables
        
        info("Multi-country configuration loaded",
             component="main",
             countries=args.countries,
             consolidated_variables=consolidated_variables,
             global_naming_used=True)
        
        # PHASE 1: GLOBAL DATA DOWNLOAD (once for all countries)
        if not args.skip_download:
            info("Starting global data download phase",
                 component="download",
                 variables=consolidated_variables)
            
            # Check if copernicus_config has the new country-specific structure
            copernicus_config = global_configs["copernicus_config"]
            
            # Check if this is the new country-specific structure
            has_country_structure = any(country in copernicus_config for country in args.countries)
            
            if has_country_structure:
                info("Using new country-specific Copernicus configuration",
                     component="download")
                
                # Get Copernicus variables from the country-specific config structure
                copernicus_variables_data = get_copernicus_variables_from_country_configs(
                    copernicus_config, 
                    args.countries
                )
                
                # Use consolidated Copernicus variables for download
                copernicus_consolidated_variables = copernicus_variables_data['consolidated']
                
                # Modify copernicus config to download only consolidated variables
                modified_copernicus_config = modify_copernicus_config_for_global_download(
                    copernicus_config, 
                    args.countries,
                    copernicus_consolidated_variables
                )
            else:
                info("Using traditional Copernicus configuration",
                     component="download")
                
                # Use original modify_config_for_variables function
                modified_copernicus_config = modify_config_for_variables(
                    copernicus_config, 
                    consolidated_variables
                )
            
            # Download ERA5 data globally
            copernicus_downloader = CopernicusDownloader(
                config=modified_copernicus_config,
                start_date=args.start_date,
                end_date=args.end_date,
                download_data_path=global_paths['raw_data']
            )
            copernicus_downloader.main()
            
            # Download CHIRPS data globally (if precipitation is in consolidated variables)
            if 'Precipitation' in consolidated_variables:
                chirps_downloader = ChirpsDownloader(
                    config=global_configs["chirps_config"],
                    start_date=args.start_date,
                    end_date=args.end_date,
                    download_data_path=global_paths['raw_data']
                )
                chirps_downloader.main()
            
            info("Global data download completed", 
                 component="download",
                 variables=consolidated_variables)
        
        # PHASE 2: PROCESS EACH COUNTRY INDIVIDUALLY
        info("Starting individual country processing phase",
             component="main",
             countries=args.countries)
        
        for country in args.countries:
            country_upper = country.upper()
            # Use global naming config for all countries
            country_config = {'naming_config': global_naming_config}
            country_variables = country_specific_variables[country_upper]
            current_country_paths = country_paths[country_upper]
            
            info(f"Processing country {country}",
                 component="main",
                 country=country,
                 variables=country_variables)
            
            try:
                process_country(
                    country=country,
                    country_config=country_config,
                    country_variables=country_variables,
                    global_configs=global_configs,
                    global_paths=global_paths,
                    country_paths=current_country_paths,
                    args=args
                )
                
                info(f"Country {country} processed successfully",
                     component="main",
                     country=country)
                     
            except Exception as e:
                error(f"Failed to process country {country}",
                      component="main",
                      country=country,
                      error=str(e))
                # Continue with other countries instead of failing completely
                warning(f"Skipping country {country} due to error, continuing with remaining countries",
                       component="main",
                       country=country)
                continue
        
        # PHASE 3: GLOBAL CLEANUP
        if not args.no_cleanup:
            info("Starting global cleanup phase", component="cleanup")
            clean_directory(global_paths['raw_data'], True)
            info("Global cleanup completed", component="cleanup")
        
        info("Multi-country ETL pipeline completed successfully", 
             component="main",
             countries=args.countries)
    
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

def modify_config_for_variables(copernicus_config: Dict[str, Any], variables: List[str]) -> Dict[str, Any]:
    """Modify copernicus configuration to only download specified variables."""
    
    info("Modifying copernicus config for consolidated variables",
         component="config",
         variables=variables)
    
    # Deep copy to avoid modifying original config
    modified_config = json.loads(json.dumps(copernicus_config))
    
    # Filter variables in each dataset
    for dataset_name, dataset_config in modified_config['datasets'].items():
        original_variables = list(dataset_config['variables'].keys())
        filtered_variables = {}
        
        for var_name, var_config in dataset_config['variables'].items():
            if var_name in variables:
                filtered_variables[var_name] = var_config
        
        dataset_config['variables'] = filtered_variables
        
        info(f"Dataset {dataset_name} filtered",
             component="config",
             dataset=dataset_name,
             original_variables=original_variables,
             filtered_variables=list(filtered_variables.keys()))
    
    info("Copernicus config modification completed",
         component="config",
         total_variables=len(variables))
    
    return modified_config


def main():
    args = parse_args()
    run_etl_pipeline(args)

if __name__ == "__main__":
    main()