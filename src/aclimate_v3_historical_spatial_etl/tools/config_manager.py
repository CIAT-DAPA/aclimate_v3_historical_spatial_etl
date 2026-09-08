"""
Configuration management utilities for ETL pipeline.
"""
import json
from pathlib import Path
from typing import Dict, Any, List, Union, Tuple
from aclimate_v3_orm.services import (
    MngDataSourceService,
    MngCountryService,
    MngCountryClimateMeasureService,
    MngClimateMeasureService,
)
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


# ---------------------------------------------------------------------------
# Country climate measures (variables) resolution driven by the ORM.
# This mirrors the "indicators by country" pattern already used by
# IndicatorsProcessor (mng_country_indicator) but for climate measures
# (mng_country_climate_measure -> mng_climate_measure).
#
# The measure's short_name (e.g. "tmax", "prec") is the canonical variable
# key used by copernicus_config/chirps_config/local_data_config. The "full
# name" (output_dir, e.g. "2m_Maximum_Temperature") is what file/directory
# naming and the geoserver_config stores fallback use.
# ---------------------------------------------------------------------------

# Temporalities used to map spatial_climate_conf entries to pipeline stages.
DAILY_TEMPORALITY = "daily"
MONTHLY_TEMPORALITY = "monthly"
CLIMATOLOGY_TEMPORALITY = "climatology"


def _normalize_conf_entry(entry: Any) -> Dict[str, Any]:
    """Normalize a spatial_climate_conf entry (dict or pydantic model) to a dict."""
    if isinstance(entry, dict):
        return entry
    if hasattr(entry, "model_dump"):
        try:
            return entry.model_dump()
        except Exception:
            pass
    if hasattr(entry, "__dict__"):
        return dict(vars(entry))
    if hasattr(entry, "__iter__"):
        return dict(entry)
    return {}


def build_variable_sources(configs: Dict[str, Any]) -> Dict[str, Any]:
    """Build short_name -> {output_dir, provider} from copernicus/chirps configs.

    Args:
        configs: Loaded configurations dictionary.

    Returns:
        A dict with:
            output_dir_by_short_name: {short_name: output_dir (full name)}
            providers: {'copernicus': [short_names...], 'chirps': [short_names...]}
    """
    result = {
        "output_dir_by_short_name": {},
        "providers": {"copernicus": [], "chirps": []},
    }

    # Copernicus variables live under datasets[default_dataset].variables
    try:
        default_dataset = configs["copernicus_config"].get("default_dataset")
        dataset_config = configs["copernicus_config"].get("datasets", {}).get(default_dataset, {})
        copernicus_vars = dataset_config.get("variables", {})
        for short_name, var_config in copernicus_vars.items():
            result["output_dir_by_short_name"][short_name] = var_config.get("output_dir", short_name)
            result["providers"]["copernicus"].append(short_name)
    except Exception as e:
        warning("Could not parse copernicus_config while building variable sources",
                component="config",
                error=str(e))

    # CHIRPS variables are the dataset keys themselves
    try:
        chirps_vars = configs["chirps_config"].get("datasets", {})
        for short_name, var_config in chirps_vars.items():
            result["output_dir_by_short_name"][short_name] = var_config.get("output_dir", short_name)
            result["providers"]["chirps"].append(short_name)
    except Exception as e:
        warning("Could not parse chirps_config while building variable sources",
                component="config",
                error=str(e))

    return result


def get_country_climate_measures(country_name: str) -> List[Dict[str, Any]]:
    """Query the DB for the country's measures enabled for spatial climate.

    Equivalent to IndicatorsProcessor._get_country_indicators() but reading
    mng_country_climate_measure (spatial_climate=True).

    Args:
        country_name: Country name, e.g. "HONDURAS".

    Returns:
        List of dicts:
            {measure_id, short_name, name, spatial_climate_conf, location_climate_conf}

    Raises:
        ETLError: if the country is not found in the database.
    """
    country_service = MngCountryService()
    ccm_service = MngCountryClimateMeasureService()
    measure_service = MngClimateMeasureService()

    countries = country_service.get_by_name(country_name)
    if not countries:
        raise ETLError(f"Country '{country_name}' not found in database (mng_country)")

    country_id = countries[0].id

    measures = ccm_service.get_by_country(country_id)
    measures = [m for m in measures if getattr(m, "spatial_climate", False)]

    result = []
    for m in measures:
        measure_id = getattr(m, "measure_id", None)
        short_name = None
        name = None

        # Prefer the nested relationship when available...
        measure_obj = getattr(m, "measure", None)
        if measure_obj is not None:
            short_name = getattr(measure_obj, "short_name", None)
            name = getattr(measure_obj, "name", None)

        # ...otherwise fetch the climate measure by id (robust fallback).
        if not short_name and measure_id:
            try:
                measure_read = measure_service.get_by_id(measure_id)
                if measure_read:
                    short_name = measure_read.short_name
                    name = measure_read.name
            except Exception as e:
                warning("Failed to load climate measure details by id",
                        component="config",
                        measure_id=measure_id,
                        error=str(e))

        result.append({
            "measure_id": measure_id,
            "short_name": short_name,
            "name": name,
            "spatial_climate_conf": [_normalize_conf_entry(e) for e in (getattr(m, "spatial_climate_conf", None) or [])],
            "location_climate_conf": getattr(m, "location_climate_conf", None),
        })

    return result


def get_country_variables_config(country_name: str, configs: Dict[str, Any]) -> Dict[str, Any]:
    """Determine which variables must run for a country on a spatial climate run.

    The source of truth is mng_country_climate_measure (spatial_climate=True),
    analogous to how indicators are resolved per country (mng_country_indicator).
    Each enabled measure is enriched with its short_name (canonical download key)
    and its output_dir (full name used for file/directory naming) resolved from
    the provider configs.

    If the country has no rows configured in the DB (or they cannot be read),
    falls back to the legacy behavior (all variables in naming_config) so the
    flows that already work keep working.

    Returns:
        {
          "source": "db" | "config",
          "measures": [
              {measure_id, short_name, output_dir, spatial_climate_conf, location_climate_conf}, ...
          ],
          "short_names": [...],   # canonical keys, e.g. ["tmax", "prec"]
          "output_dirs": [...],   # full names, e.g. ["2m_Maximum_Temperature"]
        }
    """
    sources = build_variable_sources(configs)
    output_dir_by_short_name = sources["output_dir_by_short_name"]

    # 1) Try the DB (new model: mng_country_climate_measure).
    try:
        measures = get_country_climate_measures(country_name)
    except Exception as e:
        warning("Could not load country climate measures from DB - falling back to config variables",
                component="config",
                country=country_name,
                error=str(e))
        measures = []

    if measures:
        final_measures = []
        for m in measures:
            short_name = m.get("short_name")
            if not short_name:
                warning("Skipping country climate measure without short_name",
                        component="config",
                        country=country_name,
                        measure_id=m.get("measure_id"))
                continue
            output_dir = output_dir_by_short_name.get(short_name)
            if not output_dir:
                warning("Skipping country climate measure without a matching provider variable config",
                        component="config",
                        country=country_name,
                        short_name=short_name,
                        measure_id=m.get("measure_id"))
                continue
            final_measures.append({
                "measure_id": m.get("measure_id"),
                "short_name": short_name,
                "output_dir": output_dir,
                "spatial_climate_conf": m.get("spatial_climate_conf") or [],
                "location_climate_conf": m.get("location_climate_conf"),
            })

        if final_measures:
            info("Country climate measures loaded from DB (mng_country_climate_measure)",
                 component="config",
                 country=country_name,
                 count=len(final_measures),
                 short_names=[f["short_name"] for f in final_measures],
                 output_dirs=[f["output_dir"] for f in final_measures])
            return {
                "source": "db",
                "measures": final_measures,
                "short_names": [f["short_name"] for f in final_measures],
                "output_dirs": [f["output_dir"] for f in final_measures],
            }

        warning("No spatial_climate measures with provider config were found - falling back to config variables",
                component="config",
                country=country_name)

    # 2) Fallback: legacy behavior = all variables from naming_config.
    variable_mapping = configs["naming_config"]["file_naming"]["components"]["variable_mapping"]
    fallback_measures = []
    for output_dir, short_name in variable_mapping.items():
        fallback_measures.append({
            "measure_id": None,
            "short_name": short_name,
            "output_dir": output_dir,
            "spatial_climate_conf": [],
            "location_climate_conf": None,
        })

    warning("Using fallback variables from naming_config (no DB country climate measures)",
            component="config",
            country=country_name,
            output_dirs=[f["output_dir"] for f in fallback_measures])
    return {
        "source": "config",
        "measures": fallback_measures,
        "short_names": [f["short_name"] for f in fallback_measures],
        "output_dirs": [f["output_dir"] for f in fallback_measures],
    }


def resolve_store_workspace(spatial_climate_conf: List[Dict[str, Any]],
                            temporality: str,
                            geoserver_section: Dict[str, Any],
                            output_dir: str) -> Tuple[Any, Any]:
    """Resolve (workspace, store) for a given pipeline temporality.

    Priority:
        1. DB spatial_climate_conf entry matching `temporality` (with store+workspace).
        2. geoserver_config section (workspace + stores[output_dir]) as fallback.

    Args:
        spatial_climate_conf: list of {temporality, store, workspace} from the DB model.
        temporality: "daily", "monthly" or "climatology".
        geoserver_section: geoserver_config["raw_data"|"monthly_data"|"climatology_data"].
        output_dir: full variable name used as key in geoserver_section["stores"].

    Returns:
        (workspace, store). store may be None when nothing is configured.
    """
    if spatial_climate_conf:
        for entry in spatial_climate_conf:
            conf_entry = _normalize_conf_entry(entry)
            if conf_entry.get("temporality") == temporality and conf_entry.get("store") and conf_entry.get("workspace"):
                info("Store/workspace resolved from DB country climate measure config",
                     component="geoserver",
                     temporality=temporality,
                     store=conf_entry.get("store"),
                     workspace=conf_entry.get("workspace"))
                return conf_entry.get("workspace"), conf_entry.get("store")

    workspace = (geoserver_section or {}).get("workspace")
    store = (geoserver_section or {}).get("stores", {}).get(output_dir)
    if workspace and store:
        warning("Store/workspace not in DB country climate measure config - using geoserver_config fallback",
                component="geoserver",
                temporality=temporality,
                output_dir=output_dir,
                store=store,
                workspace=workspace)
    return workspace, store