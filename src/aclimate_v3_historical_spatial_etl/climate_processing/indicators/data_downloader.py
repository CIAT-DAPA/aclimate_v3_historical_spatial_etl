import os
import requests
import numpy as np
import xarray as xr
import rasterio
from pathlib import Path
from typing import Optional, List, Dict, Tuple, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode
from rasterio.io import MemoryFile
from datetime import datetime, timedelta
from ...tools import error, warning, info


class IndicatorDataDownloader:
    """
    Downloads daily raster data from GeoServer for climate indicator calculations.
    
    This class provides functionality to download daily temperature and precipitation
    data from GeoServer using WCS (Web Coverage Service) requests.
    """

    def __init__(
        self,
        geoserver_workspace: str,
        geoserver_layer: str,
        output_path: Union[str, Path],
        variable: str,
        year_range: Tuple[str, str],
        parallel_downloads: int = 4
    ):
        """
        Initialize the indicator data downloader.
        
        Args:
            geoserver_workspace: Workspace name in GeoServer
            geoserver_layer: Layer name in GeoServer (mosaic name)
            output_path: Path to save downloaded data
            variable: Variable name (e.g., '2m_Maximum_Temperature')
            year_range: Tuple with start and end years (e.g., ('2020', '2023'))
            parallel_downloads: Number of parallel download threads
        """
        try:
            # Validate and get GeoServer configuration from environment
            self._validate_geoserver_envs()
            self.geoserver_url = os.getenv('GEOSERVER_URL', 'https://geo.aclimate.org/geoserver/').rstrip('/')
            self.geoserver_user = os.getenv('GEOSERVER_USER')
            self.geoserver_password = os.getenv('GEOSERVER_PASSWORD')
            
            # Store configuration
            self.geoserver_workspace = geoserver_workspace
            self.geoserver_layer = geoserver_layer  # This is the mosaic name
            self.variable = variable
            self.year_range = year_range
            self.parallel_downloads = int(os.getenv('MAX_PARALLEL_DOWNLOADS', parallel_downloads))
            
            # Convert path to Path object
            self.output_path = Path(output_path) if isinstance(output_path, str) else output_path
            self.output_path.mkdir(parents=True, exist_ok=True)

            info("IndicatorDataDownloader initialized successfully",
                 component="indicator_downloader",
                 geoserver_workspace=geoserver_workspace,
                 geoserver_layer=geoserver_layer,
                 variable=variable,
                 year_range=year_range,
                 output_path=str(self.output_path))
                
        except Exception as e:
            error("Failed to initialize IndicatorDataDownloader",
                  component="indicator_downloader",
                  error=str(e))
            raise

    def _validate_geoserver_envs(self):
        """Validate required GeoServer environment variables"""
        try:
            geoserver_url = os.getenv('GEOSERVER_URL')
            if not geoserver_url:
                warning("GEOSERVER_URL not set, using default: https://geo.aclimate.org/geoserver/",
                       component="indicator_downloader")
            
            has_user = bool(os.getenv('GEOSERVER_USER'))
            has_password = bool(os.getenv('GEOSERVER_PASSWORD'))
            
            if has_user != has_password:
                warning("GeoServer credentials partially configured - may require authentication",
                       component="indicator_downloader")

            info("GeoServer environment variables validated",
                 component="indicator_downloader",
                 geoserver_url=geoserver_url or "https://geo.aclimate.org/geoserver/",
                 has_credentials=has_user and has_password)
                
        except Exception as e:
            error("GeoServer environment validation failed",
                  component="indicator_downloader",
                  error=str(e))
            raise

    def _generate_date_range(self, year: int) -> List[str]:
        """
        Generate all dates for a given year.
        
        Args:
            year: Year to generate dates for
            
        Returns:
            List of dates in YYYY-MM-DD format
        """
        dates = []
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31)
        
        current_date = start_date
        while current_date <= end_date:
            dates.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
            
        return dates

    def _download_single_date(self, date: str) -> Optional[Tuple[str, np.ndarray, dict]]:
        """
        Download data for a single date.
        
        Args:
            date: Date in YYYY-MM-DD format
            
        Returns:
            Tuple of (date, array, spatial_info) or None if failed
        """
        try:
            # Construct WCS URL
            # Remove 'rest/' from geoserver_url if present
            cleaned_url = self.geoserver_url.rstrip('/').replace('/rest', '')
            base_url = f"{cleaned_url}/{self.geoserver_workspace}/ows?"
            params = {
                "service": "WCS",
                "request": "GetCoverage",
                "version": "2.0.1",
                "coverageId": self.geoserver_layer,
                "format": "image/geotiff",
                "subset": f"Time(\"{date}T00:00:00.000Z\")"
            }
            url = base_url + urlencode(params)
            
            # Prepare authentication
            auth = None
            if self.geoserver_user and self.geoserver_password:
                auth = (self.geoserver_user, self.geoserver_password)
            
            # Make request
            response = requests.get(url, auth=auth, timeout=60)
            
            # Check for 404 (no data for this date)
            if response.status_code == 404:
                warning(f"No data found for date {date}",
                       component="indicator_downloader")
                return None
                
            response.raise_for_status()
            
            # Process the raster data
            with MemoryFile(response.content) as memfile:
                with memfile.open() as raster:
                    raster_array = raster.read(1)
                    spatial_info = raster.profile
                    
            info(f"Successfully downloaded data for {date}",
                 component="indicator_downloader",
                 date=date,
                 shape=raster_array.shape)
                 
            return (date, raster_array, spatial_info)
            
        except Exception as e:
            error(f"Failed to download data for {date}: {str(e)}",
                  component="indicator_downloader",
                  date=date,
                  error=str(e))
            return None

    def download_year_data(self, year: int) -> Optional[xr.Dataset]:
        """
        Download all daily data for a specific year and return as xarray Dataset.
        
        Args:
            year: Year to download data for
            
        Returns:
            xarray Dataset with daily data or None if failed
        """
        try:
            info(f"Starting download for year {year}",
                 component="indicator_downloader",
                 year=year,
                 variable=self.variable)
            
            # Generate all dates for the year
            dates = self._generate_date_range(year)
            
            # Download data in parallel
            all_arrays = []
            all_dates = []
            spatial_info = None
            
            with ThreadPoolExecutor(max_workers=self.parallel_downloads) as executor:
                # Submit all download tasks
                future_to_date = {
                    executor.submit(self._download_single_date, date): date 
                    for date in dates
                }
                
                # Collect results
                for future in as_completed(future_to_date):
                    result = future.result()
                    if result is not None:
                        date, array, info_dict = result
                        all_arrays.append(array)
                        all_dates.append(date)
                        if spatial_info is None:
                            spatial_info = info_dict
            
            if not all_arrays:
                warning(f"No data downloaded for year {year}",
                       component="indicator_downloader",
                       year=year)
                return None
            
            # Sort by date to ensure proper order
            date_array_pairs = list(zip(all_dates, all_arrays))
            date_array_pairs.sort(key=lambda x: x[0])
            
            sorted_dates = [pair[0] for pair in date_array_pairs]
            sorted_arrays = [pair[1] for pair in date_array_pairs]
            
            # Create xarray Dataset
            data_array = np.stack(sorted_arrays, axis=0)
            
            # Create coordinate arrays
            height, width = sorted_arrays[0].shape
            transform = spatial_info['transform']
            
            # Calculate lat/lon coordinates
            lons = np.array([transform.c + (i + 0.5) * transform.a for i in range(width)])
            lats = np.array([transform.f + (j + 0.5) * transform.e for j in range(height)])
            
            # Convert date strings to datetime objects
            time_coords = [datetime.strptime(date, '%Y-%m-%d') for date in sorted_dates]
            
            # Create xarray Dataset
            dataset = xr.Dataset({
                self.variable: (['time', 'lat', 'lon'], data_array)
            }, coords={
                'time': time_coords,
                'lat': lats,
                'lon': lons
            })
            
            # Add attributes
            dataset[self.variable].attrs = {
                'units': 'degrees_celsius' if 'temperature' in self.variable.lower() else 'unknown',
                'long_name': self.variable.replace('_', ' ').title(),
                'source': f'GeoServer {self.geoserver_workspace}:{self.geoserver_layer}'
            }
            
            dataset.attrs = {
                'title': f'{self.variable} daily data for {year}',
                'source': f'Downloaded from {self.geoserver_url}',
                'variable': self.variable,
                'year': year,
                'spatial_resolution': f'{abs(transform.a):.6f} degrees',
                'crs': str(spatial_info.get('crs', 'EPSG:4326'))
            }
            
            info(f"Successfully created dataset for year {year}",
                 component="indicator_downloader",
                 year=year,
                 variable=self.variable,
                 shape=data_array.shape,
                 num_days=len(sorted_dates))
            
            return dataset
            
        except Exception as e:
            error(f"Failed to download data for year {year}",
                  component="indicator_downloader",
                  year=year,
                  error=str(e))
            return None

    def download_all_years(self) -> Dict[int, xr.Dataset]:
        """
        Download data for all years in the specified range.
        
        Returns:
            Dictionary mapping year to xarray Dataset
        """
        try:
            start_year = int(self.year_range[0])
            end_year = int(self.year_range[1])
            
            info(f"Starting download for years {start_year}-{end_year}",
                 component="indicator_downloader",
                 start_year=start_year,
                 end_year=end_year,
                 variable=self.variable)
            
            datasets = {}
            
            for year in range(start_year, end_year + 1):
                dataset = self.download_year_data(year)
                if dataset is not None:
                    datasets[year] = dataset
                else:
                    warning(f"No dataset created for year {year}",
                           component="indicator_downloader",
                           year=year)
            
            info(f"Download completed. Got data for {len(datasets)} years",
                 component="indicator_downloader",
                 requested_years=list(range(start_year, end_year + 1)),
                 successful_years=list(datasets.keys()))
            
            return datasets
            
        except Exception as e:
            error("Failed to download all years data",
                  component="indicator_downloader",
                  error=str(e))
            return {}

    def save_datasets(self, datasets: Dict[int, xr.Dataset]) -> List[Path]:
        """
        Save datasets to NetCDF files.
        
        Args:
            datasets: Dictionary mapping year to xarray Dataset
            
        Returns:
            List of saved file paths
        """
        try:
            saved_files = []
            
            for year, dataset in datasets.items():
                filename = f"{self.variable}_{year}.nc"
                filepath = self.output_path / filename
                
                # Save to NetCDF
                dataset.to_netcdf(filepath)
                saved_files.append(filepath)
                
                info(f"Saved dataset for year {year}",
                     component="indicator_downloader",
                     year=year,
                     filepath=str(filepath),
                     file_size=f"{filepath.stat().st_size / (1024*1024):.2f} MB")
            
            return saved_files
            
        except Exception as e:
            error("Failed to save datasets",
                  component="indicator_downloader",
                  error=str(e))
            return []