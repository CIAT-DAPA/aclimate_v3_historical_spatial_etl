import os
import xarray as xr
import numpy as np
import rasterio
from pathlib import Path
from typing import Optional, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.aclimate_v3_historical_spatial_etl.climate_processing.indicators.percentile_calculator import TemperaturePercentileCalculator
from src.aclimate_v3_historical_spatial_etl.climate_processing.indicators.data_downloader import IndicatorDataDownloader
from src.aclimate_v3_historical_spatial_etl.tools import info, error, warning


class TX90pCalculator(TemperaturePercentileCalculator):
    """
    Calculator for TX90p indicator: Warm days percentage.
    
    TX90p represents the percentage of days with maximum temperature > 90th percentile
    of the base period (1981-2010). This indicator measures the frequency of extreme heat.
    """

    INDICATOR_CODE = "TX90p"
    SUPPORTED_TEMPORALITIES = ["annual"]  # Only annual for now
    
    @property
    def required_percentiles(self) -> list:
        return [90]
    
    def calculate_annual(self) -> bool:
        """
        Calculate annual TX90p values.
        
        This method calculates the percentage of days with maximum temperature
        above the 90th percentile for each year in the specified date range.
        
        Returns:
            bool: True if calculation was successful, False otherwise
        """
        try:
            info("Starting TX90p annual calculation",
                 component="tx90p_calculator",
                 start_date=self.start_date,
                 end_date=self.end_date,
                 country_code=self.country_code,
                 base_period=f"{self.base_period_start}-{self.base_period_end}")
            
            # Parse year range from dates
            start_year = self.start_date[:4]
            end_year = self.end_date[:4]
            
            # Setup data downloader
            geoserver_config = self._get_geoserver_config()
            if not geoserver_config:
                return False
            
            # Get base period percentiles (cached if available)
            percentiles_dict = self.get_base_period_percentiles()
            if not percentiles_dict or 90 not in percentiles_dict:
                error("Failed to get base period 90th percentile",
                      component="tx90p_calculator")
                return False
            
            percentile_90 = percentiles_dict[90]
            
            # Get datasets for indicator calculation (reusing base period data when possible)
            datasets = self.get_datasets_for_indicator_calculation(start_year, end_year)
            
            if not datasets:
                error("Failed to get datasets for TX90p calculation",
                      component="tx90p_calculator")
                return False
            
            # Calculate TX90p for each year in parallel
            results = {}
            max_workers = int(os.getenv('MAX_PARALLEL_DOWNLOADS', 4))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_year = {
                    executor.submit(self._calculate_tx90p_for_year, year, dataset, percentile_90): year
                    for year, dataset in datasets.items()
                }
                
                for future in as_completed(future_to_year):
                    year = future_to_year[future]
                    try:
                        result = future.result()
                        if result is not None:
                            results[year] = result
                            info("TX90p calculated for year",
                                 component="tx90p_calculator",
                                 year=year)
                    except Exception as e:
                        error("Failed to calculate TX90p for year",
                              component="tx90p_calculator",
                              year=year,
                              error=str(e))
            
            # Save results
            if results:
                success = self._save_tx90p_results(results, datasets)
                
                # Clean up temporary download directory
                try:
                    temp_dir = self.output_path / "temp_downloads"
                    if temp_dir.exists():
                        import shutil
                        shutil.rmtree(temp_dir)
                        info("Temporary download directory cleaned up",
                             component="tx90p_calculator",
                             temp_dir=str(temp_dir))
                except Exception as e:
                    warning("Failed to clean up temporary directory",
                            component="tx90p_calculator",
                            temp_dir=str(temp_dir),
                            error=str(e))
                
                return success
            else:
                error("No TX90p results calculated",
                      component="tx90p_calculator")
                return False
                
        except Exception as e:
            error("Failed to calculate TX90p annual values",
                  component="tx90p_calculator",
                  indicator_code=self.INDICATOR_CODE,
                  error=str(e))
            
            # Clean up temporary download directory even on error
            try:
                temp_dir = self.output_path / "temp_downloads"
                if temp_dir.exists():
                    import shutil
                    shutil.rmtree(temp_dir)
                    info("Temporary download directory cleaned up after error",
                         component="tx90p_calculator",
                         temp_dir=str(temp_dir))
            except Exception as cleanup_error:
                warning("Failed to clean up temporary directory after error",
                        component="tx90p_calculator",
                        temp_dir=str(temp_dir),
                        error=str(cleanup_error))
            
            return False

    def _get_geoserver_config(self) -> dict:
        """Get GeoServer configuration for temperature data"""
        try:
            # Configuration for temperature data
            workspace = f"climate_historical_daily"
            layer = f"climate_historical_daily_{self.country_code}_tmax"
            store = f"climate_historical_daily_{self.country_code}_tmax"
            
            return {
                'workspace': workspace,
                'layer': layer,
                'store': store
            }
        except Exception as e:
            error("Failed to get GeoServer configuration",
                  component="tx90p_calculator",
                  error=str(e))
            return {}



    def _calculate_tx90p_for_year(self, year: int, dataset: xr.Dataset, percentile_90: np.ndarray) -> Optional[np.ndarray]:
        """
        Calculate TX90p values for a specific year using the base period percentile.
        
        Args:
            year: Year to calculate TX90p for
            dataset: xarray Dataset with daily temperature data
            percentile_90: 2D array with 90th percentile values for each pixel
            
        Returns:
            numpy array with TX90p percentage values, or None if calculation fails
        """
        try:
            info("Calculating TX90p for year",
                 component="tx90p_calculator",
                 year=year,
                 dataset_shape=dataset['2m_Maximum_Temperature'].shape)
            
            # Get temperature data
            temp_data = dataset['2m_Maximum_Temperature']
            temp_values = temp_data.values
            
            # Convert temperature to Celsius if needed
            if np.nanmean(temp_values) > 200:
                temp_values = temp_values - 273.15
                info("Converted temperature from Kelvin to Celsius",
                     component="tx90p_calculator",
                     year=year)
            
            # Calculate TX90p for each pixel
            tx90p_values = self._calculate_warm_days_percentage(temp_values, percentile_90)
            
            # Handle cases where all values were NaN
            all_nan_mask = np.all(np.isnan(temp_values), axis=0)
            tx90p_values[all_nan_mask] = np.nan
            
            info("TX90p calculation completed for year",
                 component="tx90p_calculator",
                 year=year,
                 max_tx90p=np.nanmax(tx90p_values),
                 min_tx90p=np.nanmin(tx90p_values),
                 mean_tx90p=np.nanmean(tx90p_values))
            
            return tx90p_values
            
        except Exception as e:
            error("Failed to calculate TX90p for year",
                  component="tx90p_calculator",
                  year=year,
                  error=str(e))
            return None

    def _calculate_warm_days_percentage(self, temp_values: np.ndarray, percentile_90: np.ndarray) -> np.ndarray:
        """
        Calculate percentage of days with temperature above 90th percentile for each pixel.
        
        Args:
            temp_values: 3D array (time, lat, lon) with temperature values
            percentile_90: 2D array (lat, lon) with 90th percentile values
            
        Returns:
            2D array (lat, lon) with percentage values
        """
        time_steps, height, width = temp_values.shape
        tx90p_result = np.zeros((height, width), dtype=np.float32)
        
        # Process each pixel
        for i in range(height):
            for j in range(width):
                pixel_temp = temp_values[:, i, j]
                pixel_percentile = percentile_90[i, j]
                
                # Skip if percentile is NaN or all values are NaN
                if np.isnan(pixel_percentile) or np.all(np.isnan(pixel_temp)):
                    tx90p_result[i, j] = np.nan
                    continue
                
                # Count valid temperature values
                valid_temp = pixel_temp[~np.isnan(pixel_temp)]
                
                if len(valid_temp) == 0:
                    tx90p_result[i, j] = np.nan
                    continue
                
                # Count days above 90th percentile
                warm_days = np.sum(valid_temp > pixel_percentile)
                total_days = len(valid_temp)
                
                # Calculate percentage
                if total_days > 0:
                    tx90p_result[i, j] = (warm_days / total_days) * 100.0
                else:
                    tx90p_result[i, j] = np.nan
        
        return tx90p_result

    def _save_tx90p_results(self, results: dict, datasets: dict) -> bool:
        """
        Save TX90p calculation results to GeoTIFF files.
        
        Args:
            results: Dictionary mapping years to TX90p arrays
            datasets: Dictionary mapping years to xarray Datasets (for spatial info)
            
        Returns:
            bool: True if saving was successful
        """
        try:
            info("Saving TX90p results",
                 component="tx90p_calculator",
                 year_count=len(results))
            
            # For each year, save as GeoTIFF
            for year, tx90p_data in results.items():
                output_filename = self._generate_climate_index_filename(year)
                output_path = self.output_path / output_filename
                
                # Save as GeoTIFF using spatial info from dataset
                dataset = datasets[year]
                self._save_as_geotiff(tx90p_data, output_path, year, dataset)
                
                info("TX90p result saved",
                     component="tx90p_calculator",
                     year=year,
                     output_file=str(output_path))
            
            return True
            
        except Exception as e:
            error("Failed to save TX90p results",
                  component="tx90p_calculator",
                  error=str(e))
            return False

    def _save_as_geotiff(self, data: np.ndarray, output_path: Path, year: int, dataset: xr.Dataset):
        """
        Save data as GeoTIFF with proper georeferencing from dataset.
        
        Args:
            data: 2D numpy array with TX90p values
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
                    INDICATOR='TX90p',
                    YEAR=str(year),
                    DESCRIPTION=f'Percentage of days with maximum temperature > 90th percentile of base period ({self.base_period_start}-{self.base_period_end})',
                    UNITS='percent',
                    BASE_PERIOD=f'{self.base_period_start}-{self.base_period_end}',
                    CREATED=datetime.now().isoformat()
                )
                
            info("TX90p data saved as GeoTIFF",
                 component="tx90p_calculator",
                 output_path=str(output_path),
                 year=year,
                 shape=data.shape)
                
        except Exception as e:
            error("Failed to save TX90p data as GeoTIFF",
                  component="tx90p_calculator",
                  output_path=str(output_path),
                  year=year,
                  error=str(e))
            raise

    def calculate_monthly(self) -> bool:
        """
        Monthly TX90p calculation (not implemented yet).

        This would calculate the percentage of warm days for each month.
        """
        warning("Monthly TX90p calculation not implemented",
               component="tx90p_calculator",
               indicator_code=self.INDICATOR_CODE)
        return False


class TX90pDataProcessor:
    """
    Helper class for TX90p data processing operations.
    
    This class could contain methods for:
    - Loading daily temperature data from various sources
    - Calculating percentiles for base periods
    - Data quality checks and filtering
    - Coordinate system transformations
    - Metadata handling
    """
    
    @staticmethod
    def calculate_base_period_percentile(daily_data: xr.Dataset, percentile: float = 90.0) -> xr.Dataset:
        """
        Calculate percentile for base period daily temperature data.
        
        Args:
            daily_data: Daily temperature dataset for base period
            percentile: Percentile value to calculate (default 90.0)
            
        Returns:
            xr.Dataset: Percentile values for each grid point
        """
        # TODO: Implement percentile calculation
        # This would calculate the specified percentile for each grid point
        # across all days in the base period
        pass
    
    @staticmethod
    def calculate_percentage_above_threshold(daily_data: xr.Dataset, thresholds: xr.Dataset) -> xr.Dataset:
        """
        Calculate percentage of days above threshold for each year.
        
        Args:
            daily_data: Daily temperature dataset
            thresholds: Threshold values for each grid point
            
        Returns:
            xr.Dataset: Annual percentage values
        """
        # TODO: Implement percentage calculation
        # This would:
        # 1. Compare daily values with thresholds
        # 2. Count days above threshold for each year
        # 3. Calculate percentage
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