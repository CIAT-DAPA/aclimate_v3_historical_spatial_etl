import os
import cdsapi
from zipfile import ZipFile
import calendar
from datetime import datetime
import xarray as xr
import rioxarray
import multiprocessing
import shutil
from rasterio.enums import Resampling
from pathlib import Path
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..tools import error, warning, info, RasterResampler

class CopernicusDownloader:
    def __init__(self, config: Dict,
                 start_date: str, end_date: str, 
                 download_data_path: str, keep_nc_files: bool = False,
                 local_data_connector=None):
        """
        Enhanced ERA5 data processor with support for multiple datasets and formats.
        
        Args:
            config: Dictionary with Copernicus configuration
            start_date: Start date (YYYY-MM)
            end_date: End date (YYYY-MM)
            download_data_path: Temporary download directory
            keep_nc_files: Whether to preserve NC files (default: False)
            local_data_connector: Optional LocalDataConnector for saving downloaded files
        """
        self.config = config
        self.start_date = start_date
        self.end_date = end_date
        self.download_data_path = Path(download_data_path)
        self.keep_nc_files = keep_nc_files
        self.local_data_connector = local_data_connector
        
        # Configure parallel processing
        self.max_workers = int(os.getenv('MAX_PARALLEL_DOWNLOADS', 4))

        self.validate_cdsapirc()

        # Initialize resampler for ERA5 data resolution adjustment
        try:
            self.resampler = RasterResampler()
            info("RasterResampler initialized for ERA5 data processing",
                 component="downloader",
                 target_resolution=self.resampler.target_resolution)
        except Exception as e:
            warning("RasterResampler initialization failed - proceeding without resampling",
                    component="downloader",
                    error=str(e))
            self.resampler = None

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

    def validate_cdsapirc(self):
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

    def _cleanup_netcdf_files_deferred(self, files_to_delete: List[Path]):
        """
        Safely delete NetCDF files with retry logic and proper error handling.
        This method is called after all conversions are complete to avoid conflicts.
        """
        import time
        import gc
        
        if not files_to_delete:
            return
            
        info(f"Starting deferred cleanup of {len(files_to_delete)} NetCDF files",
             component="cleanup",
             file_count=len(files_to_delete))
        
        # Force garbage collection to ensure all file handles are closed
        gc.collect()
        
        # Add a small delay to ensure all file handles are properly released
        time.sleep(1)
        
        deleted_count = 0
        failed_count = 0
        
        for nc_file in files_to_delete:
            if not nc_file.exists():
                continue
                
            max_retries = 5
            retry_delay = 0.5  # Start with shorter delay
            
            for attempt in range(max_retries):
                try:
                    nc_file.unlink()
                    deleted_count += 1
                    info("NetCDF file deleted successfully",
                         component="cleanup",
                         file=str(nc_file))
                    break
                    
                except (PermissionError, OSError) as e:
                    if attempt < max_retries - 1:
                        warning(f"Failed to delete NetCDF file, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})",
                                component="cleanup",
                                file=str(nc_file),
                                error=str(e))
                        time.sleep(retry_delay)
                        retry_delay *= 1.5  # Gradual backoff
                        
                        # Force garbage collection between retries
                        gc.collect()
                    else:
                        failed_count += 1
                        error("Failed to delete NetCDF file after all retries",
                              component="cleanup",
                              file=str(nc_file),
                              attempts=max_retries,
                              error=str(e))
                        
                except Exception as e:
                    failed_count += 1
                    error("Unexpected error deleting NetCDF file",
                          component="cleanup",
                          file=str(nc_file),
                          error=str(e))
                    break
        
        info("Deferred NetCDF cleanup completed",
             component="cleanup",
             total_files=len(files_to_delete),
             deleted=deleted_count,
             failed=failed_count,
             success_rate=f"{(deleted_count/(deleted_count+failed_count))*100:.1f}%" if (deleted_count+failed_count) > 0 else "0%")

    def download_data(self, dataset_name: Optional[str] = None, 
                     variables: Optional[List[str]] = None,
                     days: Optional[List[str]] = None,
                     times: Optional[List[str]] = None,
                     variables_filter: Optional[List[str]] = None):
        """Download data with flexible parameters and parallel processing"""
        dataset_name = dataset_name or self.config['default_dataset']
        dataset_config = self.config['datasets'][dataset_name]
        
        # Apply variables filter if provided
        if variables_filter:
            variables_to_process = variables_filter
            info(f"Applying variables filter", 
                 component="download",
                 filtered_variables=variables_filter)
        else:
            variables_to_process = variables or list(dataset_config['variables'].keys())
        start_year, start_month = map(int, self.start_date.split('-'))
        end_year, end_month = map(int, self.end_date.split('-'))

        info(f"Starting data download {variables_to_process}",
             component="download",
             dataset=dataset_name,
             variables=variables_to_process,
             date_range=f"{self.start_date} to {self.end_date}",
             max_workers=self.max_workers)

        # Prepare download tasks
        download_tasks = []
        for variable in variables_to_process:
            var_config = dataset_config['variables'].get(variable)
            if not var_config:
                warning("Variable not found in dataset",
                       component="download",
                       dataset=dataset_name,
                       variable=variable)
                continue

            for year in range(start_year, end_year + 1):
                months = self._generate_month_range(year, start_year, start_month, end_year, end_month)
                for month in months:
                    download_tasks.append({
                        'dataset_name': dataset_name,
                        'dataset_config': dataset_config,
                        'variable': variable,
                        'var_config': var_config,
                        'year': year,
                        'month': month,
                        'custom_days': days,
                        'custom_times': times
                    })

        # Execute downloads in parallel
        successful_downloads = 0
        failed_downloads = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_task = {
                executor.submit(self._download_month, **task): task
                for task in download_tasks
            }
            
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    future.result()
                    successful_downloads += 1
                    info(f"Download completed successfully",
                         component="download",
                         variable=task['variable'],
                         year=task['year'],
                         month=task['month'])
                except Exception as e:
                    failed_downloads += 1
                    error(f"Download failed",
                          component="download",
                          variable=task['variable'],
                          year=task['year'],
                          month=task['month'],
                          error=str(e))
        
        info("Data download completed",
             component="download",
             dataset=dataset_name,
             successful=successful_downloads,
             failed=failed_downloads,
             total_tasks=len(download_tasks))


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

    def _convert_netcdf_file(self, conversion_task: Dict) -> bool:
        """Convert a single NetCDF file to TIFF format with thread-safe handling"""
        import time
        
        nc_file = conversion_task['nc_file']
        tif_file = conversion_task['tif_file']
        var_config = conversion_task['var_config']
        new_crs = conversion_task['new_crs']
        
        # Check if output file already exists
        if tif_file.exists():
            info("TIFF file already exists, skipping conversion",
                 component="conversion",
                 file=str(tif_file))
            return True
        
        # Check if input file exists and is accessible
        if not nc_file.exists():
            warning("NetCDF file does not exist",
                    component="conversion",
                    file=str(nc_file))
            return False
        
        info(f"Converting NetCDF to raster {str(nc_file)} to {str(tif_file)}",
             component="conversion",
             source=str(nc_file),
             target=str(tif_file))
        
        max_retries = 3
        retry_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                # Use context manager to ensure dataset is properly closed
                with xr.open_dataset(nc_file) as xds:
                    # Apply transformations if specified
                    if 'transform' in var_config and 'value' in var_config:
                        if var_config['transform'] == "-":
                            xds = xds - var_config['value']
                        elif var_config['transform'] == "/":
                            xds = xds / var_config['value']
                    
                    # Set coordinate reference system
                    xds.rio.write_crs(new_crs, inplace=True)
                    
                    # Find data variable
                    data_var = next((v for v in xds.variables 
                                   if v not in ['time','lat','lon','longitude','latitude','crs']), None)
                    
                    if data_var:
                        # Create temporary output file to avoid conflicts
                        temp_tif_file = tif_file.with_suffix('.tmp.tif')
                        
                        # Write to temporary file first
                        xds[data_var].rio.to_raster(temp_tif_file)
                        
                        # Atomically rename to final file
                        temp_tif_file.rename(tif_file)
                        
                        info("Raster conversion successful",
                             component="conversion",
                             file=str(tif_file))
                        
                        # Mark NetCDF file for deletion instead of deleting immediately
                        # to avoid conflicts with parallel processing
                        conversion_task['delete_nc'] = not self.keep_nc_files
                        
                        return True
                    else:
                        warning("No data variable found in NetCDF",
                                component="conversion",
                                file=str(nc_file))
                        return False
                        
            except (PermissionError, OSError) as e:
                if "being used by another process" in str(e) and attempt < max_retries - 1:
                    warning(f"File access conflict, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})",
                            component="conversion",
                            source=str(nc_file),
                            error=str(e))
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                else:
                    error("File access error after retries",
                          component="conversion",
                          source=str(nc_file),
                          attempts=attempt + 1,
                          error=str(e))
                    return False
                    
            except Exception as e:
                error("Conversion failed with unexpected error",
                      component="conversion",
                      source=str(nc_file),
                      attempt=attempt + 1,
                      error=str(e))
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                return False
        
        return False

    def netcdf_to_raster(self, variables_filter: Optional[List[str]] = None):
        """
        Convert NC to TIFF and organize files by year with parallel processing.
        Auto-deletes NC unless keep_nc_files=True.
        Saves original NC files to local repository if local_data_connector is configured.
        
        Args:
            variables_filter: List of variables to process. If None, processes all variables.
        """
        new_crs = '+proj=longlat +datum=WGS84 +no_defs'
        start_year, start_month = map(int, self.start_date.split('-'))
        end_year, end_month = map(int, self.end_date.split('-'))

        info("Starting NetCDF to raster conversion",
             component="conversion",
             keep_nc_files=self.keep_nc_files,
             max_workers=self.max_workers)
        
        # Limit conversion workers to prevent segfaults on high-core servers
        cpu_count = multiprocessing.cpu_count()
        
        # Allow manual override of conversion workers
        conversion_override = os.getenv('ERA5_CONVERSION_WORKERS')
        if conversion_override:
            conversion_workers = int(conversion_override)
            info(f"Using manual override for conversion workers: {conversion_workers}",
                 component="conversion",
                 override_workers=conversion_workers)
        elif cpu_count >= 8:
            # High core count server - force single worker to prevent GDAL crashes
            conversion_workers = 1
            warning(f"High CPU count detected ({cpu_count} cores), forcing single-threaded NetCDF conversion to prevent segfaults",
                    component="conversion",
                    cpu_count=cpu_count,
                    original_workers=self.max_workers,
                    forced_workers=conversion_workers)
        else:
            # Lower core count - safe to use normal workers
            conversion_workers = self.max_workers

        # Prepare conversion tasks
        conversion_tasks = []
        processed_files = set()  # Track already processed files to avoid duplicates
        
        for dataset_name, dataset_config in self.config['datasets'].items():
            for variable, var_config in dataset_config['variables'].items():
                # Apply variables filter if provided
                if variables_filter and variable not in variables_filter:
                    info(f"Skipping variable {variable} (not in filter)",
                         component="conversion",
                         variable=variable,
                         filter=variables_filter)
                    continue
                    
                info("Processing variable for conversion",
                     component="conversion",
                     dataset=dataset_name,
                     variable=variable)
                
                var_path = self.download_data_path / var_config['output_dir']
                
                for year in range(start_year, end_year + 1):
                    year_path = var_path / str(year)
                    if not year_path.exists():
                        continue
                    
                    # Find all unique NC files in the year directory instead of searching by day
                    nc_patterns = [
                        f"{variable}_*.nc",
                        f"*{year}*.nc"
                    ]
                    
                    nc_files = set()
                    for pattern in nc_patterns:
                        matches = year_path.glob(pattern)
                        nc_files.update(matches)
                    
                    # Process each unique NetCDF file only once
                    for nc_file in nc_files:
                        if not nc_file.exists():
                            continue
                            
                        # Create unique identifier to avoid duplicates
                        file_key = str(nc_file.resolve())
                        if file_key in processed_files:
                            continue
                        processed_files.add(file_key)
                        
                        # Extract date information from filename for TIFF naming
                        # Try to extract YYYYMMDD pattern from filename
                        import re
                        date_match = re.search(r'(\d{8})', nc_file.name)
                        if date_match:
                            date_str = date_match.group(1)
                            tif_file = year_path / f"{var_config['output_dir']}_{date_str}.tif"
                        else:
                            # Fallback to simple naming
                            base_name = nc_file.stem
                            tif_file = year_path / f"{var_config['output_dir']}_{base_name}.tif"
                        
                        conversion_tasks.append({
                            'nc_file': nc_file,
                            'tif_file': tif_file,
                            'var_config': var_config,
                            'new_crs': new_crs,
                            'dataset_name': dataset_name,
                            'variable': variable,
                            'year': year
                        })
                        
                        # Save original NC file to local repository if connector is available
                        if self.local_data_connector and self.local_data_connector.config.get('enabled', False):
                            # Extract date from filename for local repository naming
                            import re
                            date_match = re.search(r'(\d{8})', nc_file.name)
                            if date_match:
                                date_str = date_match.group(1)
                                # Convert to YYYY-MM-DD format
                                formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                                success = self.local_data_connector.save_downloaded_file(
                                    str(nc_file), variable, formatted_date
                                )
                                if success:
                                    info(f"Saved {variable} file to local repository",
                                         component="local_save",
                                         variable=variable,
                                         date=formatted_date,
                                         file=nc_file.name)

        # Execute conversions in parallel
        total_conversions = len(conversion_tasks)
        successful_conversions = 0
        failed_conversions = 0
        files_to_delete = []  # Collect NetCDF files for deferred deletion
        
        info(f"Starting parallel conversion of {total_conversions} files",
             component="conversion",
             total_tasks=total_conversions,
             max_workers=conversion_workers)
        
        with ThreadPoolExecutor(max_workers=conversion_workers) as executor:
            future_to_task = {
                executor.submit(self._convert_netcdf_file, task): task
                for task in conversion_tasks
            }
            
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    success = future.result()
                    if success:
                        successful_conversions += 1
                        # Collect files marked for deletion
                        if task.get('delete_nc', False):
                            files_to_delete.append(task['nc_file'])
                    else:
                        failed_conversions += 1
                except Exception as e:
                    failed_conversions += 1
                    error("Conversion task failed",
                          component="conversion",
                          dataset=task['dataset_name'],
                          variable=task['variable'],
                          year=task['year'],
                          source=str(task['nc_file']),
                          error=str(e))

        # Deferred cleanup of NetCDF files after all conversions are complete
        if files_to_delete:
            self._cleanup_netcdf_files_deferred(files_to_delete)

        # Organize remaining NC files for each dataset/variable
        for dataset_name, dataset_config in self.config['datasets'].items():
            for variable, var_config in dataset_config['variables'].items():
                var_path = self.download_data_path / var_config['output_dir']
                
                for year in range(start_year, end_year + 1):
                    year_path = var_path / str(year)
                    if year_path.exists():
                        self._organize_nc_files(year_path)

        info("NetCDF to raster conversion completed",
             component="conversion",
             total_processed=total_conversions,
             successful=successful_conversions,
             failed=failed_conversions,
             success_rate=f"{(successful_conversions/total_conversions)*100:.1f}%" if total_conversions > 0 else "0%")

    def resample_rasters(self, variables_filter: Optional[List[str]] = None):
        """
        Resample all generated TIFF files to match CHIRPS resolution with parallel processing.
        Files are resampled in place to maintain the same naming and location.
        """
        if not self.resampler:
            warning("RasterResampler not available - skipping resampling step",
                    component="resampling")
            return

        start_year, start_month = map(int, self.start_date.split('-'))
        end_year, end_month = map(int, self.end_date.split('-'))

        info("Starting raster resampling for ERA5 data",
             component="resampling",
             target_resolution=self.resampler.target_resolution,
             max_workers=self.max_workers)

        # Collect all TIFF files that need resampling
        all_tiff_files = []
        
        for dataset_name, dataset_config in self.config['datasets'].items():
            for variable, var_config in dataset_config['variables'].items():
                # Apply variables filter if provided
                if variables_filter and variable not in variables_filter:
                    info(f"Skipping variable {variable} resampling (not in filter)",
                         component="resampling",
                         variable=variable,
                         filter=variables_filter)
                    continue
                    
                info("Collecting files for resampling",
                     component="resampling",
                     dataset=dataset_name,
                     variable=variable)
                
                var_path = self.download_data_path / var_config['output_dir']
                
                for year in range(start_year, end_year + 1):
                    year_path = var_path / str(year)
                    if not year_path.exists():
                        continue
                    
                    # Find all TIFF files in the year directory
                    tiff_files = list(year_path.glob("*.tif"))
                    
                    if tiff_files:
                        all_tiff_files.extend(tiff_files)
                        info(f"Found {len(tiff_files)} TIFF files for {year}",
                             component="resampling",
                             year=year,
                             variable=variable,
                             file_count=len(tiff_files))

        if not all_tiff_files:
            info("No TIFF files found for resampling",
                 component="resampling")
            return

        info(f"Total TIFF files to resample: {len(all_tiff_files)}",
             component="resampling",
             total_files=len(all_tiff_files))

        # Use parallel directory resampling for better performance
        # Group files by their parent directories
        dir_groups = {}
        for tiff_file in all_tiff_files:
            parent_dir = tiff_file.parent
            if parent_dir not in dir_groups:
                dir_groups[parent_dir] = []
            dir_groups[parent_dir].append(tiff_file)

        total_successful = 0
        total_failed = 0
        total_skipped = 0

        # Process each directory group
        for directory, files_in_dir in dir_groups.items():
            info(f"Resampling {len(files_in_dir)} files in {directory.name}",
                 component="resampling",
                 directory=str(directory),
                 file_count=len(files_in_dir))

            # Create temporary directory for resampled files
            temp_resample_dir = directory / "temp_resampled"
            temp_resample_dir.mkdir(exist_ok=True)

            try:
                # Prepare file pairs for parallel resampling
                file_pairs = []
                for tiff_file in files_in_dir:
                    temp_output = temp_resample_dir / tiff_file.name
                    file_pairs.append((tiff_file, temp_output))

                # Use the new parallel resampling method
                summary = self.resampler.resample_files_parallel(
                    file_pairs=file_pairs,
                    resampling_method=Resampling.bilinear,
                    overwrite=True
                )

                # Move successfully resampled files back to original location
                for tiff_file in files_in_dir:
                    temp_file = temp_resample_dir / tiff_file.name
                    if temp_file.exists():
                        # Replace original with resampled version
                        tiff_file.unlink()
                        temp_file.rename(tiff_file)

                total_successful += summary["successful"]
                total_failed += summary["failed"] 
                total_skipped += summary["skipped"]

                info(f"Directory resampling completed",
                     component="resampling",
                     directory=str(directory),
                     **summary)

            except Exception as e:
                error("Directory resampling failed",
                      component="resampling",
                      directory=str(directory),
                      error=str(e))
                total_failed += len(files_in_dir)

            finally:
                # Clean up temporary directory
                try:
                    if temp_resample_dir.exists():
                        import shutil
                        shutil.rmtree(temp_resample_dir)
                except Exception as cleanup_error:
                    warning("Failed to clean up temporary directory",
                            component="resampling",
                            temp_dir=str(temp_resample_dir),
                            error=str(cleanup_error))

        total_processed = len(all_tiff_files)
        info("Raster resampling completed",
             component="resampling",
             total_processed=total_processed,
             successful=total_successful,
             failed=total_failed,
             skipped=total_skipped,
             success_rate=f"{(total_successful/total_processed)*100:.1f}%" if total_processed > 0 else "0%")

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

    def main(self, variables_filter=None):
        """
        Main processing pipeline
        
        Args:
            variables_filter: List of variables to download (e.g., ['tmax', 'tmin']). 
                            If None, downloads all configured variables.
        """
        try:
            info("Starting Copernicus downloader main pipeline", component="main")
            self.download_data(variables_filter=variables_filter)
            self.netcdf_to_raster(variables_filter=variables_filter)
            
            # Resample ERA5 data to match CHIRPS resolution
            self.resample_rasters(variables_filter=variables_filter)
            
            info("Copernicus downloader pipeline completed successfully", component="main")
        except Exception as e:
            error("Copernicus downloader pipeline failed",
                  component="main",
                  error=str(e))
            raise