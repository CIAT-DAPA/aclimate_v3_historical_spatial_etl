import os
import cdsapi
from zipfile import ZipFile
import calendar
from datetime import datetime
import xarray as xr
import rioxarray
import shutil
from pathlib import Path
from typing import Dict, List, Optional
from ..tools import error, warning, info

class CopernicusDownloader:
    def __init__(self, config: Dict,
                 start_date: str, end_date: str, 
                 download_data_path: str, keep_nc_files: bool = False):
        """
        Enhanced ERA5 data processor with support for multiple datasets and formats.
        
        Args:
            config: Dictionary with Copernicus configuration
            start_date: Start date (YYYY-MM)
            end_date: End date (YYYY-MM)
            download_data_path: Temporary download directory
            keep_nc_files: Whether to preserve NC files (default: False)
        """
        self.config = config
        self.start_date = start_date
        self.end_date = end_date
        self.download_data_path = Path(download_data_path)
        self.keep_nc_files = keep_nc_files

        self.validate_cdsapirc()

        self._initialize_paths()
        info(f"CopernicusDownloader initialized {self.start_date} to {self.end_date}", 
            component="downloader",
            date_range=f"{self.start_date} to {self.end_date}")

    def _initialize_paths(self):
        try:
            for dataset in self.config['datasets'].values():
                for var_config in dataset['variables'].values():
                    path = self.download_data_path / var_config['output_dir']
                    path.mkdir(parents=True, exist_ok=True)
            info("Output directories initialized",
                 component="setup",
                 base_path=str(self.download_data_path))
        except Exception as e:
            error("Failed to initialize output directories",
                  component="setup",
                  error=str(e))
            raise
    
    def _validate_paths(self):
        """Ensure base directory exists"""
        try:
            self.download_data_path.mkdir(parents=True, exist_ok=True)
            info("Directory structure validated",
                 component="setup",
                 path=str(self.download_data_path))
        except Exception as e:
            error("Failed to validate directory structure",
                  component="setup",
                  path=str(self.download_data_path),
                  error=str(e))
            raise

    def validate_cdsapirc():
        """
        Validates the existence and format of ~/.cdsapirc for Copernicus CDS API authentication.
        Raises FileNotFoundError or ValueError if invalid.
        """
        cdsapirc_path = Path.home() / ".cdsapirc"
        if not cdsapirc_path.exists():
            error(f"The {cdsapirc_path} file was not found. It is required for authentication with the Copernicus CDS API.", component="setup", path=str(cdsapirc_path))
            raise FileNotFoundError(f"The {cdsapirc_path} file was not found. Please create this file with your Copernicus credentials.")
        # Validate format
        with open(cdsapirc_path, "r") as f:
            lines = [line.strip() for line in f if line.strip()]
        url_line = next((l for l in lines if l.lower().startswith("url:")), None)
        key_line = next((l for l in lines if l.lower().startswith("key:")), None)
        if not url_line or not key_line:
            error(f"The {cdsapirc_path} file is missing required 'url' or 'key' entries.", component="setup", path=str(cdsapirc_path))
            raise ValueError(f"The {cdsapirc_path} file is invalid. It must contain both 'url:' and 'key:' entries.")
        # Optionally, check url format
        url = url_line.split("url:",1)[-1].strip()
        key = key_line.split("key:",1)[-1].strip()
        if not url.startswith("https://") or not key:
            error(f"The {cdsapirc_path} file has an invalid url or key format.", component="setup", path=str(cdsapirc_path), url=url, key=key)
            raise ValueError(f"The {cdsapirc_path} file has an invalid url or key format.")
        info(f"{cdsapirc_path} file validated successfully.", component="setup", path=str(cdsapirc_path))

    def _organize_nc_files(self, year_path: Path):
        """Move NC files to nc/ subfolder if keep_nc_files is True"""
        if not self.keep_nc_files:
            return
            
        nc_folder = year_path / "nc"
        try:
            nc_folder.mkdir(exist_ok=True)
            moved_files = 0
            
            for nc_file in year_path.glob("*.nc"):
                try:
                    shutil.move(str(nc_file), str(nc_folder / nc_file.name))
                    moved_files += 1
                except Exception as e:
                    warning("Failed to move NC file",
                            component="cleanup",
                            file=str(nc_file),
                            error=str(e))
            
            info("NC files organized",
                 component="cleanup",
                 path=str(nc_folder),
                 files_moved=moved_files)
        except Exception as e:
            warning("Failed to organize NC files",
                    component="cleanup",
                    path=str(year_path),
                    error=str(e))

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

        info(f"Starting data download {variables_to_process}",
             component="download",
             dataset=dataset_name,
             variables=variables_to_process,
             date_range=f"{self.start_date} to {self.end_date}")

        for variable in variables_to_process:
            var_config = dataset_config['variables'].get(variable)
            if not var_config:
                warning("Variable not found in dataset",
                       component="download",
                       dataset=dataset_name,
                       variable=variable)
                continue

            info(f"Processing variable {variable}",
                 component="download",
                 variable=variable,
                 dataset=dataset_name)
            
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
        
        info("Data download completed",
             component="download",
             dataset=dataset_name)


    def _build_request(self, dataset_name: str, dataset_config: Dict, var_config: Dict,
                       year: int, month: str, days: List[str],
                       custom_times: Optional[List[str]] = None) -> Dict:
        try:
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

            # Handle format parameters
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

            info("Request parameters built",
                 component="request",
                 dataset=dataset_name,
                 variable=var_config['name'],
                 year=year,
                 month=month)
            return request

        except Exception as e:
            error("Failed to build request parameters",
                  component="request",
                  dataset=dataset_name,
                  variable=var_config.get('name'),
                  error=str(e))
            raise

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
            cds_client = cdsapi.Client(timeout=800)
            if dataset_config.get('format', '') == 'zip' or dataset_config.get('download_format', '') == 'zip':
                zip_file = output_dir / f"{variable}_{year}{month}.zip"
                info("Starting zip file download",
                     component="download",
                     file=str(zip_file))
                
                cds_client.retrieve(dataset_name, request, str(zip_file))
                
                info("Extracting zip file",
                     component="download",
                     file=str(zip_file))
                with ZipFile(zip_file, 'r') as z:
                    z.extractall(output_dir)
                zip_file.unlink()
                info("Zip file processed and deleted",
                     component="download",
                     file=str(zip_file))
            else:
                ext = dataset_config.get('format', 'nc')
                filename = f"{variable}_{year}{month}.{ext}"
                output_path = output_dir / filename
                info("Starting file download",
                     component="download",
                     file=str(output_path))
                
                cds_client.retrieve(dataset_name, request, str(output_path))
                
                info("File download completed",
                     component="download",
                     file=str(output_path),
                     size=f"{os.path.getsize(output_path)/1024/1024:.2f}MB")
        
        except Exception as e:
            error("Download failed",
                  component="download",
                  dataset=dataset_name,
                  variable=variable,
                  year=year,
                  month=month,
                  error=str(e))

    def netcdf_to_raster(self):
        """
        Convert NC to TIFF and organize files by year.
        Auto-deletes NC unless keep_nc_files=True.
        """
        new_crs = '+proj=longlat +datum=WGS84 +no_defs'
        start_year, start_month = map(int, self.start_date.split('-'))
        end_year, end_month = map(int, self.end_date.split('-'))

        info("Starting NetCDF to raster conversion",
             component="conversion",
             keep_nc_files=self.keep_nc_files)

        for dataset_name, dataset_config in self.config['datasets'].items():
            for variable, var_config in dataset_config['variables'].items():
                info("Processing variable for conversion",
                     component="conversion",
                     dataset=dataset_name,
                     variable=variable)
                
                var_path = self.download_data_path / var_config['output_dir']
                files_converted = 0
                files_failed = 0
                
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
                                info(f"Converting NetCDF to raster {str(nc_file)} to {str(tif_file)}",
                                     component="conversion",
                                     source=str(nc_file),
                                     target=str(tif_file))
                                
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
                                    files_converted += 1
                                    info("Raster conversion successful",
                                         component="conversion",
                                         file=str(tif_file))
                                    
                                    # Handle NC file
                                    if not self.keep_nc_files:
                                        nc_file.unlink()
                                        info("Source NetCDF file deleted",
                                             component="cleanup",
                                             file=str(nc_file))
                                else:
                                    warning("No data variable found in NetCDF",
                                            component="conversion",
                                            file=str(nc_file))
                                    files_failed += 1
                            
                            except Exception as e:
                                error("Conversion failed",
                                      component="conversion",
                                      source=str(nc_file),
                                      error=str(e))
                                files_failed += 1
                    
                    # Organize remaining NC files
                    self._organize_nc_files(year_path)
                
                info("Variable conversion completed",
                     component="conversion",
                     dataset=dataset_name,
                     variable=variable,
                     files_converted=files_converted,
                     files_failed=files_failed)

        info("NetCDF to raster conversion completed",
             component="conversion")

    def clean_rasters(self):
        """Optional cleanup of all generated TIFF files"""
        info("Starting raster cleanup", component="cleanup")
        files_deleted = 0
        dirs_removed = 0
        errors = 0

        for dataset_config in self.config['datasets'].values():
            for var_config in dataset_config['variables'].values():
                var_path = self.download_data_path / var_config['output_dir']
                
                if var_path.exists():
                    # Delete all TIFF files
                    for tif_file in var_path.glob("**/*.tif"):
                        try:
                            tif_file.unlink()
                            files_deleted += 1
                        except Exception as e:
                            error(f"Failed to delete file {str(tif_file)}",
                                  component="cleanup",
                                  file=str(tif_file),
                                  error=str(e))
                            errors += 1
                    
                    # Clean empty directories
                    for year_dir in var_path.glob("*"):
                        if year_dir.is_dir() and not any(year_dir.iterdir()):
                            try:
                                year_dir.rmdir()
                                dirs_removed += 1
                            except Exception as e:
                                error("Failed to remove directory",
                                      component="cleanup",
                                      dir=str(year_dir),
                                      error=str(e))
                                errors += 1

        info("Raster cleanup completed",
             component="cleanup",
             files_deleted=files_deleted,
             directories_removed=dirs_removed,
             errors_encountered=errors)

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
        try:
            info("Starting Copernicus downloader main pipeline", component="main")
            self.download_data()
            self.netcdf_to_raster()
            info("Copernicus downloader pipeline completed successfully", component="main")
        except Exception as e:
            error("Copernicus downloader pipeline failed",
                  component="main",
                  error=str(e))
            raise