"""
Download pipeline utilities for ETL operations.
"""
from pathlib import Path
from typing import Dict, List, Any, TYPE_CHECKING
from .logging_manager import error, warning, info

if TYPE_CHECKING:
    from ..connectors import CopernicusDownloader, ChirpsDownloader, LocalDataConnector


def execute_download_pipeline(args, configs: Dict[str, Any], paths: Dict[str, Path], 
                            local_data_connector=None) -> bool:
    """
    Execute the download pipeline with local data integration.
    
    Args:
        args: Command line arguments
        configs: Configuration dictionary
        paths: Directory paths dictionary
        local_data_connector: Optional local data connector
        
    Returns:
        True if successful, False otherwise
    """
    # Import inside function to avoid circular imports
    from ..connectors import CopernicusDownloader, ChirpsDownloader
    
    try:
        info("Starting data download phase", component="download")
        
        # Extract variables from configurations
        copernicus_variables = list(configs["copernicus_config"]["datasets"][configs["copernicus_config"]["default_dataset"]]["variables"].keys())
        chirps_variables = list(configs["chirps_config"]["datasets"].keys())
        
        info("Variables extracted from configurations",
             component="download",
             copernicus_variables=copernicus_variables,
             chirps_variables=chirps_variables)
        
        # Check local data availability if local connector is enabled
        variables_to_download = {'copernicus': copernicus_variables, 'chirps': chirps_variables}
        
        if local_data_connector and local_data_connector.config.get('enabled', False):
            info("Checking local data availability", component="download")
            
            # Check availability for all variables
            local_availability = local_data_connector.get_available_variables(args.start_date, args.end_date)
            
            # Copy available local files to raw_data directory
            for variable, availability in local_availability.items():
                for date_str in availability['available_locally']:
                    # Pass base raw_data path - LocalDataConnector will handle proper structure
                    success = local_data_connector.copy_local_file(variable, date_str, str(paths['raw_data']))
                    if success:
                        info(f"Copied local file for {variable} {date_str}", 
                            component="download",
                            variable=variable,
                            date=date_str)
            
            # Update variables to download (only missing ones)
            copernicus_missing = []
            chirps_missing = []
            
            for variable in copernicus_variables:
                if variable in local_availability and local_availability[variable]['missing_locally']:
                    copernicus_missing.append(variable)
            
            for variable in chirps_variables:
                if variable in local_availability and local_availability[variable]['missing_locally']:
                    chirps_missing.append(variable)
            
            variables_to_download = {'copernicus': copernicus_missing, 'chirps': chirps_missing}
            
            info("Local data availability check completed",
                 component="download",
                 copernicus_missing=copernicus_missing,
                 chirps_missing=chirps_missing)
        
        # Initialize downloaders
        copernicus_downloader = None
        
        # Download Copernicus data (only missing variables)
        if variables_to_download['copernicus']:
            copernicus_downloader = CopernicusDownloader(
                config=configs["copernicus_config"],
                start_date=args.start_date,
                end_date=args.end_date,
                download_data_path=paths['raw_data'],
                local_data_connector=local_data_connector
            )
            copernicus_downloader.main(variables_filter=variables_to_download['copernicus'])
        else:
            info("Skipping Copernicus download - all data available locally", component="download")

        # Process Copernicus data (convert nc→tif and resample) if any Copernicus files are present
        # This includes both downloaded files and files copied from local repository
        if copernicus_variables:
            has_copernicus_files = False
            
            # Check if we have any .nc files to process
            for variable in copernicus_variables:
                var_config = configs["copernicus_config"]["datasets"][configs["copernicus_config"]["default_dataset"]]["variables"][variable]
                var_path = paths['raw_data'] / var_config['output_dir']
                if var_path.exists() and list(var_path.glob("**/*.nc")):
                    has_copernicus_files = True
                    break
            
            if has_copernicus_files:
                info("Processing Copernicus data (nc→tif conversion and resampling)", component="download")
                if not copernicus_downloader:
                    # Create downloader for processing only (no downloads)
                    copernicus_downloader = CopernicusDownloader(
                        config=configs["copernicus_config"],
                        start_date=args.start_date,
                        end_date=args.end_date,
                        download_data_path=paths['raw_data'],
                        local_data_connector=local_data_connector
                    )
                
                # Process all available Copernicus variables (not just downloaded ones)
                copernicus_downloader.netcdf_to_raster(variables_filter=copernicus_variables)
                copernicus_downloader.resample_rasters(variables_filter=copernicus_variables)
                info("Copernicus data processing completed", component="download")
            else:
                info("No Copernicus .nc files found to process", component="download")

        # Download CHIRPS data (only missing variables)
        if variables_to_download['chirps']:
            chirps_downloader = ChirpsDownloader(
                config=configs["chirps_config"],
                start_date=args.start_date,
                end_date=args.end_date,
                download_data_path=paths['raw_data'],
                local_data_connector=local_data_connector
            )
            chirps_downloader.main()
        else:
            info("Skipping CHIRPS download - all data available locally", component="download")
        
        info("Data download completed", component="download")
        return True
        
    except Exception as e:
        error(f"Download pipeline failed: {str(e)}",
              component="download",
              error=str(e))
        return False