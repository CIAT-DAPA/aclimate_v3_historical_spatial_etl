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
import shutil

class CopernicusDownloader:
    def __init__(self, config_path: str,
                 start_date: str, end_date: str, 
                 download_data_path: str, keep_nc_files: bool = False):
        """
        Enhanced ERA5 data processor with support for multiple datasets and formats.
        
        Args:
            config_path: Path to configuration file
            start_date: Start date (YYYY-MM)
            end_date: End date (YYYY-MM)
            download_data_path: Temporary download directory
            keep_nc_files: Whether to preserve NC files (default: False)
        """
        self.config = self._load_config(config_path)
        self.start_date = start_date
        self.end_date = end_date
        self.download_data_path = Path(download_data_path)
        self.keep_nc_files = keep_nc_files
        
        self._initialize_paths()
        self.cds_client = cdsapi.Client(timeout=600)

    def _load_config(self, config_path: str) -> Dict:
        with open(config_path) as f:
            return json.load(f)

    def _validate_paths(self):
        """Ensure base directory exists"""
        self.download_data_path.mkdir(parents=True, exist_ok=True)

    def _organize_nc_files(self, year_path: Path):
        """Move NC files to nc/ subfolder if keep_nc_files is True"""
        if not self.keep_nc_files:
            return
            
        nc_folder = year_path / "nc"
        nc_folder.mkdir(exist_ok=True)
        
        for nc_file in year_path.glob("*.nc"):
            try:
                shutil.move(str(nc_file), str(nc_folder / nc_file.name))
                print(f"\tMoved NC file to: {nc_folder/nc_file.name}")
            except Exception as e:
                print(f"\tError moving NC file: {str(e)}")

    def _initialize_paths(self):

        for dataset in self.config['datasets'].values():
            for var_config in dataset['variables'].values():
                (self.download_data_path / var_config['output_dir']).mkdir(parents=True, exist_ok=True)

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
        Convert NC to TIFF and organize files by year.
        Auto-deletes NC unless keep_nc_files=True.
        """
        new_crs = '+proj=longlat +datum=WGS84 +no_defs'
        start_year, start_month = map(int, self.start_date.split('-'))
        end_year, end_month = map(int, self.end_date.split('-'))

        for dataset_name, dataset_config in self.config['datasets'].items():
            for variable, var_config in dataset_config['variables'].items():
                print(f"\nProcessing variable: {variable}")
                
                var_path = self.download_data_path / var_config['output_dir']
                
                for year in range(start_year, end_year + 1):
                    year_path = var_path / str(year)
                    if not year_path.exists():
                        continue
                    
                    months = self._generate_month_range(year, start_year, start_month, end_year, end_month)
                    
                    for month in months:
                        days = self._generate_days(year, int(month))
                        
                        for day in days:
                            # Find NC file (supports multiple naming patterns)
                            nc_patterns = [
                                f"{variable}_{year}{month}.nc",
                                f"{var_config.get('file_name','')}*{year}{month}{day}*.nc"
                            ]
                            
                            nc_file = None
                            for pattern in nc_patterns:
                                matches = list(year_path.glob(pattern))
                                if matches:
                                    nc_file = matches[0]
                                    break
                            
                            if not nc_file or not nc_file.exists():
                                continue
                                
                            # Generate TIFF path
                            tif_file = year_path / f"{var_config['output_dir']}_{year}{month}{day}.tif"
                            
                            try:
                                # Conversion logic
                                xds = xr.open_dataset(nc_file)
                                
                                if 'transform' in var_config and 'value' in var_config:
                                    if var_config['transform'] == "-":
                                        xds = xds - var_config['value']
                                    elif var_config['transform'] == "/":
                                        xds = xds / var_config['value']
                                
                                xds.rio.write_crs(new_crs, inplace=True)
                                
                                # Find data variable
                                data_var = next((v for v in xds.variables 
                                               if v not in ['time','lat','lon','longitude','latitude','crs']), None)
                                
                                if data_var:
                                    xds[data_var].rio.to_raster(tif_file)
                                    print(f"Generated: {tif_file}")
                                    
                                    # Handle NC file
                                    if not self.keep_nc_files:
                                        nc_file.unlink()
                                else:
                                    print(f"No data variable in: {nc_file}")
                            
                            except Exception as e:
                                print(f"Error processing {nc_file}: {str(e)}")
                    
                    # Organize remaining NC files
                    self._organize_nc_files(year_path)

    def clean_rasters(self):
        """Optional cleanup of all generated TIFF files"""
        print("\nCleaning all raster files...")
        for dataset_config in self.config['datasets'].values():
            for var_config in dataset_config['variables'].values():
                var_path = self.download_data_path / var_config['output_dir']
                
                if var_path.exists():
                    # Delete all TIFF files
                    for tif_file in var_path.glob("**/*.tif"):
                        try:
                            tif_file.unlink()
                            print(f"Deleted: {tif_file}")
                        except Exception as e:
                            print(f"Error deleting {tif_file}: {str(e)}")
                    
                    # Clean empty directories
                    for year_dir in var_path.glob("*"):
                        if year_dir.is_dir() and not any(year_dir.iterdir()):
                            try:
                                year_dir.rmdir()
                                print(f"Removed empty: {year_dir}")
                            except Exception as e:
                                print(f"Error removing {year_dir}: {str(e)}")


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
        self.download_data()
        self.netcdf_to_raster()