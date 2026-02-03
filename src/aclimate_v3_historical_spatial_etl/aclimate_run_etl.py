
import argparse
from pathlib import Path
import sys
from .connectors import LocalDataConnector
from .tools import (
    RasterClipper, GeoServerUploadPreparer, logging_manager, error, info, warning,
    force_cleanup_resources, clean_directory, setup_directory_structure,
    load_config_with_iso2, get_variables_from_config, validate_dates,
    validate_indicator_years, execute_download_pipeline, ETLError
)
from .climate_processing import MonthlyProcessor, ClimatologyProcessor, IndicatorsProcessor
from aclimate_v3_orm.database.base import create_tables


#python -m src.aclimate_v3_historical_spatial_etl.aclimate_run_etl --country HONDURAS --start_date 2025-04 --end_date 2025-04 --data_path "D:\\Code\\aclimate_v3_historical_spatial_etl\\data_test"
#python -m src.aclimate_v3_historical_spatial_etl.aclimate_run_etl --country HONDURAS --skip_processing --indicators --indicator_years 1981 --data_path "D:\\Code\\aclimate_v3_historical_spatial_etl\\data_test"
def parse_args():
    """Parse simplified command line arguments."""
    info("Parsing command line arguments", component="setup")
    parser = argparse.ArgumentParser(description="Climate Data ETL Pipeline")
    
    # Required arguments
    parser.add_argument("--country", required=True, help="Country name for processing")
    parser.add_argument("--start_date", help="Start date in YYYY-MM format (required unless using --skip_processing with --indicators)")
    parser.add_argument("--end_date", help="End date in YYYY-MM format (required unless using --skip_processing with --indicators)")
    parser.add_argument("--data_path", required=True, help="Base directory for all data")
    parser.add_argument("--local_data_path", help="Path to local data repository root (e.g., 'D:\\CIAT\\spatial_data_test'). If provided, enables local data validation and storage.")
    # Pipeline control flags
    parser.add_argument("--skip_download", action="store_true", help="Skip data download step")
    parser.add_argument("--skip_processing", action="store_true", help="Skip all data processing steps (download, clipping, monthly aggregation, climatology) - useful for indicators-only runs")
    parser.add_argument("--download_only", action="store_true", help="Only perform data download to feed local repository, skip all other processing")
    parser.add_argument("--climatology", action="store_true", help="Calculate climatology")
    parser.add_argument("--indicators", action="store_true", help="Calculate climate indicators")
    parser.add_argument("--indicator_years", type=str, help="Year range for indicator calculation (e.g., '2020-2023')")
    parser.add_argument("--no_cleanup", action="store_true", help="Disable automatic cleanup")
    parser.add_argument("--init", action="store_true", help="Initialize database tables before running ETL")
    
    args = parser.parse_args()
    
    # Custom validation for start_date and end_date
    indicators_only = args.skip_processing and args.indicators
    download_only = args.download_only
    
    # For download_only mode, start_date and end_date are always required
    if download_only:
        if not args.start_date:
            parser.error("--start_date is required when using --download_only")
        if not args.end_date:
            parser.error("--end_date is required when using --download_only")
        if not args.local_data_path:
            parser.error("--local_data_path is required when using --download_only")
    
    if not indicators_only and not download_only:
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
        
        # Initialize local data connector if path provided
        local_data_connector = None
        if args.local_data_path:
            try:
                local_data_connector = LocalDataConnector(
                    config=configs["local_data_config"],
                    local_data_path=args.local_data_path,
                    copernicus_config=configs["copernicus_config"],
                    chirps_config=configs["chirps_config"]
                )
                info("Local data connector initialized",
                     component="main",
                     local_data_path=args.local_data_path)
            except Exception as e:
                warning(f"Failed to initialize local data connector: {str(e)}",
                       component="main")
                local_data_connector = None
        
        # Handle download-only mode
        if args.download_only:
            info("Running in download-only mode", component="main")
            success = execute_download_pipeline(args, configs, paths, local_data_connector)
            if success:
                info("Download-only pipeline completed successfully", component="main")
            else:
                raise ETLError("Download-only pipeline failed")
            return
        
        # Step 1: Data Download with Local Data Integration
        if not args.skip_download and not args.skip_processing:
            if not args.start_date or not args.end_date:
                error("start_date and end_date are required for data download", component="download")
                raise ETLError("start_date and end_date are required when downloading data")
            
            success = execute_download_pipeline(args, configs, paths, local_data_connector)
            if not success:
                raise ETLError("Download pipeline failed")
        else:
            info("Skipping data download phase", component="download")

        
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
                force_cleanup_resources()
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
                force_cleanup_resources()
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
                force_cleanup_resources()
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
                    temporality = indicator.get('temporality', 'annual')
                    indicators_config['stores'][short_name] = f'climate_index_{temporality}_{iso2}_{short_name}'
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
                    temporality = indicator.get('temporality', 'annual')
                    store_name = f'climate_index_{temporality}_{iso2}_{indicator_short_name}'
                    warning(f"No store name configured for indicator {indicator_short_name}, using fallback",
                           component="geoserver",
                           indicator=indicator_short_name,
                           fallback_store=store_name)
                
                indicators_preparer.upload_to_geoserver(
                    workspace=indicators_config['workspace'],
                    store=store_name,
                    date_format="yyyy"
                )
                force_cleanup_resources()
                clean_directory(paths['upload_geoserver'], True)
            
            info("Indicators data GeoServer upload completed", component="geoserver")
        
        # Step 7: Cleanup
        if not args.no_cleanup:
            info("Starting cleanup phase", component="cleanup")
            
            # Force cleanup of resources before final directory cleanup
            force_cleanup_resources()
            
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