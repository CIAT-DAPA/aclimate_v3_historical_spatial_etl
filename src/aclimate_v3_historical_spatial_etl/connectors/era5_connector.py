import os
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
import cdsapi
from zipfile import ZipFile
import calendar
from datetime import datetime
import xarray as xr
import rioxarray

class CopernicusDownloader:
    def __init__(self, config_path: str, output_path: str,
                 start_date: str, end_date: str, download_data_path: str):
        """
        Enhanced ERA5 data processor with support for multiple datasets and formats.
        
        Args:
            config_path: Path to configuration file
            output_path: Base output directory
            start_date: Start date (YYYY-MM)
            end_date: End date (YYYY-MM)
            download_data_path: Temporary download directory
        """
        self.config = self._load_config(config_path)
        self.output_path = Path(output_path)
        self.start_date = start_date
        self.end_date = end_date
        self.download_data_path = Path(download_data_path)
        
        self._initialize_paths()
        self.cds_client = cdsapi.Client(timeout=600)

    def _load_config(self, config_path: str) -> Dict:
        with open(config_path) as f:
            return json.load(f)

    def _initialize_paths(self):
        self.era5_rasters_path = self.download_data_path / "rasters"
        self.era5_rasters_path.mkdir(parents=True, exist_ok=True)

        for dataset in self.config['datasets'].values():
            for var_config in dataset['variables'].values():
                (self.download_data_path / var_config['output_dir']).mkdir(parents=True, exist_ok=True)
                (self.era5_rasters_path / var_config['output_dir']).mkdir(parents=True, exist_ok=True)
                (self.output_path / var_config['output_dir']).mkdir(parents=True, exist_ok=True)

    def download_data(self, dataset_name: Optional[str] = None, 
                     variables: Optional[List[str]] = None,
                     days: Optional[List[str]] = None,
                     times: Optional[List[str]] = None):
        """Download data with flexible parameters"""
        dataset_name = dataset_name or self.config['default_dataset']
        dataset_config = self.config['datasets'][dataset_name]
        
        variables_to_process = variables or list(dataset_config['variables'].keys())
        start_year, start_month = map(int, self.start_date.split('-'))
        end_year, end_month = map(int, self.end_date.split('-'))

        for variable in variables_to_process:
            var_config = dataset_config['variables'].get(variable)
            if not var_config:
                print(f"Variable {variable} not found in dataset {dataset_name}")
                continue

            print(f"Processing {variable} from {dataset_name}")
            
            for year in range(start_year, end_year + 1):
                months = self._generate_month_range(year, start_year, start_month, end_year, end_month)
                for month in months:
                    self._download_month(
                        dataset_name=dataset_name,
                        dataset_config=dataset_config,
                        variable=variable,
                        var_config=var_config,
                        year=year,
                        month=month,
                        custom_days=days,
                        custom_times=times
                    )
    def _build_request(self, dataset_name: str, dataset_config: Dict, var_config: Dict,
                       year: int, month: str, days: List[str],
                       custom_times: Optional[List[str]] = None) -> Dict:

        request = {
            'variable': [var_config['name']],
            'year': [str(year)],
            'month': [month],
            'day': days
        }

        # Add dataset-specific base parameters if they exist
        if 'base_parameters' in dataset_config:
            request.update(dataset_config['base_parameters'])

        if 'statistics' in var_config:
            request['statistic'] = var_config['statistics']

        # Add variable-specific additional parameters
        if 'additional_params' in var_config:
            request.update(var_config['additional_params'])

        # Handle format parameters (data_format vs download_format)
        if 'format' in dataset_config:
            request['format'] = dataset_config['format']
        if 'data_format' in dataset_config:
            request['data_format'] = dataset_config['data_format']
        if 'download_format' in dataset_config:
            request['format'] = dataset_config['download_format']

        # Only add version if it's explicitly defined in the dataset config
        if 'version' in dataset_config:
            request['version'] = dataset_config['version']

        # Override times if specified
        if custom_times and 'time' in request:
            request['time'] = custom_times

        # Special handling for time format in agromet dataset
        if dataset_name == "sis-agrometeorological-indicators" and 'time' in request:
            request['time'] = [t.replace(':', '_') for t in request['time']]

        return request

    def _download_month(self, dataset_name: str, dataset_config: Dict,
                    variable: str, var_config: Dict, year: int, month: str,
                    custom_days: Optional[List[str]] = None,
                    custom_times: Optional[List[str]] = None):
        """Handle the actual download with correct parameters for each dataset"""
        days = custom_days or self._generate_days(year, int(month))
        output_dir = self.download_data_path / var_config['output_dir'] / str(year)
        output_dir.mkdir(parents=True, exist_ok=True)

        request = self._build_request(dataset_name, dataset_config, var_config,
                                    year, month, days, custom_times)

        try:
            if dataset_config.get('format', '') == 'zip' or dataset_config.get('download_format', '') == 'zip':
                zip_file = output_dir / f"{variable}_{year}{month}.zip"
                self.cds_client.retrieve(dataset_name, request, str(zip_file))
                with ZipFile(zip_file, 'r') as z:
                    z.extractall(output_dir)
                zip_file.unlink()
            else:
                ext = dataset_config.get('format', 'nc')
                filename = f"{variable}_{year}{month}.{ext}"
                self.cds_client.retrieve(dataset_name, request, str(output_dir / filename))
        
        except Exception as e:
            print(f"Error downloading {variable} {year}-{month}: {str(e)}")


    def netcdf_to_raster(self):
        """
        Converts downloaded NetCDF files to raster format (.tif) for all configured variables.
        Handles both uncompressed files and those within zip archives.
        Uses configuration to determine file naming patterns and transformations.
        """
        # Standard CRS definition for output rasters
        new_crs = '+proj=longlat +datum=WGS84 +no_defs'
        
        # Parse start and end dates
        start_year, start_month = map(int, self.start_date.split('-'))
        end_year, end_month = map(int, self.end_date.split('-'))

        # Process each dataset and variable in the configuration
        for dataset_name, dataset_config in self.config['datasets'].items():
            for variable, var_config in dataset_config['variables'].items():
                print(f"\nProcessing variable: {variable} from dataset {dataset_name}")
                
                # Path where output rasters will be saved
                raster_save_path = self.output_path / var_config['output_dir']
                raster_save_path.mkdir(parents=True, exist_ok=True)
                
                # Path where downloaded NetCDFs are stored
                download_path = self.download_data_path / var_config['output_dir']
                
                # Process each year in the date range
                for year in range(start_year, end_year + 1):
                    year_path = download_path / str(year)
                    raster_year_path = raster_save_path / str(year)
                    raster_year_path.mkdir(exist_ok=True)
                    
                    # Generate months to process based on date range
                    months = self._generate_month_range(year, start_year, start_month, end_year, end_month)
                    
                    # Process each month
                    for month in months:
                        days = self._generate_days(year, int(month))
                        
                        # Process each day
                        for day in days:
                            # Build filename pattern based on dataset
                            if dataset_name == "sis-agrometeorological-indicators":
                                file_patterns = self.config['datasets'][dataset_name]['file_patterns']
                                nc_filename = (
                                    f"{var_config['file_name']}"
                                    f"{file_patterns['ERA5_FILE']}"
                                    f"{year}{month}{day}"
                                    f"{file_patterns['ERA5_FILE_TYPE']}"
                                )
                            else:
                                # For other datasets, use variable name as base
                                nc_filename = f"{variable}_{year}{month}.nc"
                            
                            input_file = year_path / nc_filename
                            
                            # Skip if file doesn't exist
                            if not input_file.exists():
                                print(f"\tFile not found: {input_file}")
                                continue
                            
                            print(f"\tConverting {input_file} to raster...")
                            
                            # Define output raster path
                            output_file = raster_year_path / f"{var_config['output_dir']}_{year}{month}{day}.tif"
                            
                            # Skip if output already exists
                            if output_file.exists():
                                print(f"\tRaster already exists: {output_file}")
                                continue
                            
                            try:
                                # Open and process NetCDF file
                                xds = xr.open_dataset(input_file)
                                
                                # Apply transformations if defined in config
                                if 'transform' in var_config and 'value' in var_config:
                                    if var_config['transform'] == "-":
                                        xds = xds - var_config['value']
                                    elif var_config['transform'] == "/":
                                        xds = xds / var_config['value']
                                
                                # Set CRS and save as raster
                                xds.rio.write_crs(new_crs, inplace=True)
                                variable_names = list(xds.variables)
                                
                                # Find the data variable (typically the 3rd or 4th)
                                data_var = None
                                for v in variable_names:
                                    if v not in ['time', 'lat', 'lon', 'longitude', 'latitude', 'crs']:
                                        data_var = v
                                        break
                                
                                if data_var:
                                    xds[data_var].rio.to_raster(output_file)
                                    print(f"\tSaved raster to {output_file}")
                                else:
                                    print(f"\tNo data variable found in {input_file}")
                            
                            except Exception as e:
                                print(f"\tError processing {input_file}: {str(e)}")
        
        print("\nConversion complete for all variables")


    # Helper methods
    def _generate_days(self, year: int, month: int) -> List[str]:
        """Generate days for a month"""
        _, num_days = calendar.monthrange(year, month)
        return [f"{day:02d}" for day in range(1, num_days + 1)]

    def _generate_month_range(self, year: int, start_year: int, start_month: int,
                            end_year: int, end_month: int) -> List[str]:
        """Generate months for a year within date range"""
        if year == start_year and year == end_year:
            return [f"{month:02d}" for month in range(start_month, end_month + 1)]
        elif year == start_year:
            return [f"{month:02d}" for month in range(start_month, 13)]
        elif year == end_year:
            return [f"{month:02d}" for month in range(1, end_month + 1)]
        return [f"{month:02d}" for month in range(1, 13)]

    def main(self):
        """Main processing pipeline"""
        #self.download_data()
        self.netcdf_to_raster()