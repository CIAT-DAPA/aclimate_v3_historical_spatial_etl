import os
import xarray as xr
import numpy as np
import rasterio
from pathlib import Path
from typing import Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.aclimate_v3_historical_spatial_etl.climate_processing.indicators.base_calculator import BaseIndicatorCalculator
from src.aclimate_v3_historical_spatial_etl.climate_processing.indicators.data_downloader import IndicatorDataDownloader
from src.aclimate_v3_historical_spatial_etl.tools import info, error, warning


class CDDCalculator(BaseIndicatorCalculator):
    """
    Calculator for CDD indicator: Consecutive dry days.
    
    CDD represents the maximum number of consecutive days with precipitation < 1 mm
    in each year. This indicator measures the duration of dry periods.
    """

    INDICATOR_CODE = "CDD"
    SUPPORTED_TEMPORALITIES = ["annual"]  # Only annual for now
    
    def calculate_annual(self) -> bool:
        """
        Calculate annual CDD values.
        
        This method calculates the maximum number of consecutive dry days
        for each year in the specified date range.
        
        Returns:
            bool: True if calculation was successful, False otherwise
        """
        try:
            info("Starting CDD annual calculation",
                 component="cdd_calculator",
                 start_date=self.start_date,
                 end_date=self.end_date,
                 country_code=self.country_code)
            
            # Parse year range from dates
            start_year = self.start_date[:4]
            end_year = self.end_date[:4]
            
            # Setup data downloader
            geoserver_config = self._get_geoserver_config()
            if not geoserver_config:
                return False
            
            downloader = IndicatorDataDownloader(
                geoserver_workspace=geoserver_config['workspace'],
                geoserver_layer=geoserver_config['layer'],
                output_path=self.output_path / "temp_downloads",
                variable="Precipitation",
                year_range=(start_year, end_year),
                parallel_downloads=4
            )
            
            # Download data for all years
            datasets = downloader.download_all_years()
            
            if not datasets:
                error("No data downloaded",
                      component="cdd_calculator")
                return False
            
            # Calculate CDD for each year in parallel
            results = {}
            max_workers = int(os.getenv('MAX_PARALLEL_DOWNLOADS', 4))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_year = {
                    executor.submit(self._calculate_cdd_for_year, year, dataset): year
                    for year, dataset in datasets.items()
                }
                
                for future in as_completed(future_to_year):
                    year = future_to_year[future]
                    try:
                        result = future.result()
                        if result is not None:
                            results[year] = result
                            info("CDD calculated for year",
                                 component="cdd_calculator",
                                 year=year)
                    except Exception as e:
                        error("Failed to calculate CDD for year",
                              component="cdd_calculator",
                              year=year,
                              error=str(e))
            
            # Save results
            if results:
                success = self._save_cdd_results(results, datasets)
                
                # Clean up temporary download directory
                try:
                    temp_dir = self.output_path / "temp_downloads"
                    if temp_dir.exists():
                        import shutil
                        shutil.rmtree(temp_dir)
                        info("Temporary download directory cleaned up",
                             component="cdd_calculator",
                             temp_dir=str(temp_dir))
                except Exception as e:
                    warning("Failed to clean up temporary directory",
                            component="cdd_calculator",
                            temp_dir=str(temp_dir),
                            error=str(e))
                
                return success
            else:
                error("No CDD results calculated",
                      component="cdd_calculator")
                return False
                
        except Exception as e:
            error("Failed to calculate CDD annual values",
                  component="cdd_calculator",
                  indicator_code=self.INDICATOR_CODE,
                  error=str(e))
            
            # Clean up temporary download directory even on error
            try:
                temp_dir = self.output_path / "temp_downloads"
                if temp_dir.exists():
                    import shutil
                    shutil.rmtree(temp_dir)
                    info("Temporary download directory cleaned up after error",
                         component="cdd_calculator",
                         temp_dir=str(temp_dir))
            except Exception as cleanup_error:
                warning("Failed to clean up temporary directory after error",
                        component="cdd_calculator",
                        temp_dir=str(temp_dir),
                        error=str(cleanup_error))
            
            return False

    def _get_geoserver_config(self) -> dict:
        """Get GeoServer configuration for precipitation data"""
        try:
            # Configuration for precipitation data
            workspace = f"climate_historical_daily"
            layer = f"climate_historical_daily_{self.country_code}_prec"
            store = f"climate_historical_daily_{self.country_code}_prec"
            
            return {
                'workspace': workspace,
                'layer': layer,
                'store': store
            }
        except Exception as e:
            error("Failed to get GeoServer configuration",
                  component="cdd_calculator",
                  error=str(e))
            return {}

    def _calculate_cdd_for_year(self, year: int, dataset: xr.Dataset) -> Optional[np.ndarray]:
        """
        Calculate CDD values for a specific year using xarray Dataset.
        
        Args:
            year: Year to calculate CDD for
            dataset: xarray Dataset with daily precipitation data
            
        Returns:
            numpy array with CDD values, or None if calculation fails
        """
        try:
            info("Calculating CDD for year",
                 component="cdd_calculator",
                 year=year,
                 dataset_shape=dataset['Precipitation'].shape)
            
            # Get precipitation data
            precip_data = dataset['Precipitation']
            
            # Convert precipitation to mm/day if needed (check units)
            precip_values = precip_data.values
            
            # Handle invalid values (CHIRPS often uses -9999 for no data)
            # Set negative values and typical no-data values to NaN
            invalid_mask = (precip_values < 0) | (precip_values == -9999) | (precip_values > 1000)
            if np.any(invalid_mask):
                precip_values = precip_values.copy()  # Make a copy to avoid modifying original
                precip_values[invalid_mask] = np.nan
                invalid_count = np.sum(invalid_mask)
                info(f"Converted {invalid_count} invalid values to NaN (negative, -9999, or > 1000mm)",
                     component="cdd_calculator",
                     year=year,
                     invalid_count=invalid_count)
            
            # Check if values are in m/day (very small values) and convert to mm/day
            valid_values = precip_values[~np.isnan(precip_values)]
            if len(valid_values) > 0 and np.max(valid_values) < 1 and np.max(valid_values) > 0:
                precip_values = precip_values * 1000  # Convert from m to mm
                info("Converted precipitation from m to mm",
                     component="cdd_calculator",
                     year=year)
            
            # Calculate CDD for each pixel
            cdd_values = self._calculate_consecutive_dry_days(precip_values)
            
            # Handle cases where all values were NaN
            all_nan_mask = np.all(np.isnan(precip_values), axis=0)
            cdd_values[all_nan_mask] = np.nan
            
            info("CDD calculation completed for year",
                 component="cdd_calculator",
                 year=year,
                 max_cdd=np.nanmax(cdd_values),
                 min_cdd=np.nanmin(cdd_values),
                 mean_cdd=np.nanmean(cdd_values))
            
            return cdd_values
            
        except Exception as e:
            error("Failed to calculate CDD for year",
                  component="cdd_calculator",
                  year=year,
                  error=str(e))
            return None

    def _calculate_consecutive_dry_days(self, precip_values: np.ndarray) -> np.ndarray:
        """
        Calculate maximum consecutive dry days for each pixel.
        
        Args:
            precip_values: 3D array (time, lat, lon) with precipitation values
            
        Returns:
            2D array (lat, lon) with maximum consecutive dry days
        """
        time_steps, height, width = precip_values.shape
        cdd_result = np.zeros((height, width), dtype=np.float32)
        
        # Process each pixel
        for i in range(height):
            for j in range(width):
                pixel_precip = precip_values[:, i, j]
                
                # Skip if all values are NaN
                if np.all(np.isnan(pixel_precip)):
                    cdd_result[i, j] = np.nan
                    continue
                
                # Find dry days (precipitation < 1 mm)
                dry_days = pixel_precip < 1.0
                
                # Handle NaN values - treat them as not dry
                dry_days = np.where(np.isnan(pixel_precip), False, dry_days)
                
                # Calculate consecutive dry periods
                max_consecutive = 0
                current_consecutive = 0
                
                for k in range(len(dry_days)):
                    if dry_days[k]:
                        current_consecutive += 1
                        max_consecutive = max(max_consecutive, current_consecutive)
                    else:
                        current_consecutive = 0
                
                cdd_result[i, j] = max_consecutive
        
        return cdd_result

    def _save_cdd_results(self, results: dict, datasets: dict) -> bool:
        """
        Save CDD calculation results to GeoTIFF files.
        
        Args:
            results: Dictionary mapping years to CDD arrays
            datasets: Dictionary mapping years to xarray Datasets (for spatial info)
            
        Returns:
            bool: True if saving was successful
        """
        try:
            info("Saving CDD results",
                 component="cdd_calculator",
                 year_count=len(results))
            
            # For each year, save as GeoTIFF
            for year, cdd_data in results.items():
                output_filename = self._generate_climate_index_filename(year)
                output_path = self.output_path / output_filename
                
                # Save as GeoTIFF using spatial info from dataset
                dataset = datasets[year]
                self._save_as_geotiff(cdd_data, output_path, year, dataset)
                
                info("CDD result saved",
                     component="cdd_calculator",
                     year=year,
                     output_file=str(output_path))
            
            return True
            
        except Exception as e:
            error("Failed to save CDD results",
                  component="cdd_calculator",
                  error=str(e))
            return False

    def _save_as_geotiff(self, data: np.ndarray, output_path: Path, year: int, dataset: xr.Dataset):
        """
        Save data as GeoTIFF with proper georeferencing from dataset.
        
        Args:
            data: 2D numpy array with CDD values
            output_path: Output file path
            year: Year for metadata
            dataset: xarray Dataset for spatial information
        """
        try:
            height, width = data.shape
            
            # Get spatial information from dataset
            lons = dataset.lon.values
            lats = dataset.lat.values
            
            # Calculate transform from coordinates
            lon_min, lon_max = float(lons.min()), float(lons.max())
            lat_min, lat_max = float(lats.min()), float(lats.max())
            
            transform = rasterio.transform.from_bounds(
                west=lon_min, south=lat_min, east=lon_max, north=lat_max,
                width=width, height=height
            )
            
            # Get CRS from dataset attributes or use default
            crs = dataset.attrs.get('crs', 'EPSG:4326')
            
            with rasterio.open(
                output_path,
                'w',
                driver='GTiff',
                height=height,
                width=width,
                count=1,
                dtype=data.dtype,
                crs=crs,
                transform=transform,
                compress='lzw',
                nodata=np.nan
            ) as dst:
                dst.write(data, 1)
                
                # Add metadata
                dst.update_tags(
                    INDICATOR='CDD',
                    YEAR=str(year),
                    DESCRIPTION='Maximum number of consecutive dry days (precipitation < 1 mm)',
                    UNITS='days',
                    CREATED=datetime.now().isoformat()
                )
                
            info("CDD data saved as GeoTIFF",
                 component="cdd_calculator",
                 output_path=str(output_path),
                 year=year,
                 shape=data.shape)
                
        except Exception as e:
            error("Failed to save CDD data as GeoTIFF",
                  component="cdd_calculator",
                  output_path=str(output_path),
                  year=year,
                  error=str(e))
            raise

    def calculate_monthly(self) -> bool:
        """
        Monthly CDD calculation (not implemented yet).

        This would calculate the maximum consecutive dry days for each month.
        """
        warning("Monthly CDD calculation not implemented",
               component="cdd_calculator",
               indicator_code=self.INDICATOR_CODE)
        return False


class CDDDataProcessor:
    """
    Helper class for CDD data processing operations.
    
    This class could contain methods for:
    - Loading daily precipitation data from various sources
    - Data quality checks and filtering
    - Coordinate system transformations
    - Metadata handling
    """
    
    @staticmethod
    def load_daily_precipitation_data(data_path: Path, start_date: str, end_date: str) -> xr.Dataset:
        """
        Load daily precipitation data for the specified period.
        
        Args:
            data_path: Path to precipitation data files
            start_date: Start date in YYYY-MM format
            end_date: End date in YYYY-MM format
            
        Returns:
            xr.Dataset: Daily precipitation data
        """
        # TODO: Implement data loading logic
        # This could load from:
        # - NetCDF files
        # - TIFF files
        # - Database queries
        # - GeoServer WCS requests
        pass
    
    @staticmethod
    def calculate_consecutive_dry_periods(daily_data: xr.Dataset, threshold: float = 1.0) -> xr.Dataset:
        """
        Calculate consecutive dry periods from daily precipitation data.
        
        Args:
            daily_data: Daily precipitation dataset
            threshold: Precipitation threshold in mm (default 1.0 mm)
            
        Returns:
            xr.Dataset: Consecutive dry days values
        """
        # TODO: Implement consecutive dry days calculation
        # This would identify periods where precipitation < threshold
        # and find the maximum consecutive period per year
        pass
    
    @staticmethod
    def save_result(data: xr.Dataset, output_path: Path, metadata: dict) -> bool:
        """
        Save calculated results to file.
        
        Args:
            data: Calculated indicator data
            output_path: Output file path
            metadata: Metadata to include in output
            
        Returns:
            bool: True if successful
        """
        # TODO: Implement result saving
        # This could save as:
        # - GeoTIFF for raster data
        # - NetCDF for multidimensional data
        # - CSV for point data
        pass