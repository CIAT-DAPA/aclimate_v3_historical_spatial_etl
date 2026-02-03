"""
Configuration management utilities for ETL pipeline.
"""
import json
from pathlib import Path
from typing import Dict, Any, List, Union, Tuple
from aclimate_v3_orm.services import MngDataSourceService
from .logging_manager import error, warning, info


class ETLError(Exception):
    """Custom exception for ETL pipeline errors"""
    pass


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
    
    # 2.1 Configuraciones opcionales
    optional_configs = {
        "local_data_config": None
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

    # 4. Load optional configurations (don't fail if missing)
    for config_name in optional_configs.keys():
        try:
            db_config = data_source_service.get_by_name_and_country(name=f"{config_name}", country_name=country_name)
            
            if db_config and db_config.content:
                config_content = json.loads(db_config.content)
                loaded_configs[config_name] = config_content
                info(f"Optional config loaded successfully {config_name}",
                     component="setup",
                     config_name=config_name)
            else:
                # Set default config for missing optional configs
                if config_name == "local_data_config":
                    loaded_configs[config_name] = {"enabled": False}
                    info(f"Using default config for {config_name}",
                         component="setup",
                         config_name=config_name)
                
        except json.JSONDecodeError as e:
            warning(f"Invalid JSON in optional configuration {config_name}, using defaults",
                   component="setup",
                   config_name=config_name,
                   error=str(e))
            if config_name == "local_data_config":
                loaded_configs[config_name] = {"enabled": False}
        except Exception as e:
            warning(f"Failed to load optional configuration {config_name}, using defaults",
                   component="setup",
                   config_name=config_name,
                   error=str(e))
            if config_name == "local_data_config":
                loaded_configs[config_name] = {"enabled": False}

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


def extract_variables_from_configs(configs: Dict[str, Any]) -> tuple:
    """Extract Copernicus and CHIRPS variables from their respective configurations."""
    try:
        copernicus_variables = list(configs["copernicus_config"]["datasets"][configs["copernicus_config"]["default_dataset"]]["variables"].keys())
        chirps_variables = list(configs["chirps_config"]["datasets"].keys())
        
        info("Variables extracted from configurations",
             component="config",
             copernicus_variables=copernicus_variables,
             chirps_variables=chirps_variables)
        
        return copernicus_variables, chirps_variables
        
    except KeyError as e:
        error("Failed to extract variables from configurations",
              component="config",
              error=str(e))
        raise ETLError(f"Could not extract variables from configurations: {str(e)}")
    except Exception as e:
        error("Unexpected error extracting variables",
              component="config",
              error=str(e))
        raise ETLError(f"Unexpected error: {str(e)}")