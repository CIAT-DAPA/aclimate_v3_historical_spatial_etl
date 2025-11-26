import os
import xarray as xr
import numpy as np
import rasterio
from pathlib import Path
from typing import Optional, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.aclimate_v3_historical_spatial_etl.climate_processing.indicators.percentile_calculator import PrecipitationPercentileCalculator
from src.aclimate_v3_historical_spatial_etl.climate_processing.indicators.data_downloader import IndicatorDataDownloader
from src.aclimate_v3_historical_spatial_etl.tools import info, error, warning


class R95pTOTCalculator(PrecipitationPercentileCalculator):
    """
    Calculator for R95pTOT indicator: Very wet day precipitation.
    
    R95pTOT represents the total precipitation on days > 95th percentile
    of the base period (1981-2010). This indicator measures the contribution
    of extreme precipitation events to total rainfall.
    """

    INDICATOR_CODE = "R95pTOT"
    SUPPORTED_TEMPORALITIES = ["annual"]  # Only annual for now
    
    @property
    def required_percentiles(self) -> list:
        return [95]
    
    def calculate_annual(self) -> bool:
        """
        Calculate annual R95pTOT values.
        
        This method calculates the total precipitation on days above the 95th percentile
        for each year in the specified date range.
        
        Returns:
            bool: True if calculation was successful, False otherwise
        """
        try:
            info("Starting R95pTOT annual calculation",
                 component="r95ptot_calculator",
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
            if not percentiles_dict or 95 not in percentiles_dict:
                error("Failed to get base period 95th percentile",
                      component="r95ptot_calculator")
                return False
            
            percentile_95 = percentiles_dict[95]
            
            # Get datasets for indicator calculation (reusing base period data when possible)
            datasets = self.get_datasets_for_indicator_calculation(start_year, end_year)
            
            if not datasets:
                error("Failed to get datasets for R95pTOT calculation",
                      component="r95ptot_calculator")
                return False
            
            # Calculate R95pTOT for each year in parallel
            results = {}
            max_workers = int(os.getenv('MAX_PARALLEL_DOWNLOADS', 4))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_year = {
                    executor.submit(self._calculate_r95ptot_for_year, year, dataset, percentile_95): year
                    for year, dataset in datasets.items()
                }
                
                for future in as_completed(future_to_year):
                    year = future_to_year[future]
                    try:
                        result = future.result()
                        if result is not None:
                            results[year] = result
                            info("R95pTOT calculated for year",
                                 component="r95ptot_calculator",
                                 year=year)
                    except Exception as e:
                        error("Failed to calculate R95pTOT for year",
                              component="r95ptot_calculator",
                              year=year,
                              error=str(e))
            
            # Save results
            if results:
                success = self._save_r95ptot_results(results, datasets)
                
                # Clean up temporary download directory
                try:
                    temp_dir = self.output_path / "temp_downloads"
                    if temp_dir.exists():
                        import shutil
                        shutil.rmtree(temp_dir)
                        info("Temporary download directory cleaned up",
                             component="r95ptot_calculator",
                             temp_dir=str(temp_dir))
                except Exception as e:
                    warning("Failed to clean up temporary directory",
                            component="r95ptot_calculator",
                            temp_dir=str(temp_dir),
                            error=str(e))
                
                return success
            else:
                error("No R95pTOT results calculated",
                      component="r95ptot_calculator")
                return False
                
        except Exception as e:
            error("Failed to calculate R95pTOT annual values",
                  component="r95ptot_calculator",
                  indicator_code=self.INDICATOR_CODE,
                  error=str(e))
            
            # Clean up temporary download directory even on error
            try:
                temp_dir = self.output_path / "temp_downloads"
                if temp_dir.exists():
                    import shutil
                    shutil.rmtree(temp_dir)
                    info("Temporary download directory cleaned up after error",
                         component="r95ptot_calculator",
                         temp_dir=str(temp_dir))
            except Exception as cleanup_error:
                warning("Failed to clean up temporary directory after error",
                        component="r95ptot_calculator",
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
                  component="r95ptot_calculator",
                  error=str(e))
            return {}



    def _calculate_r95ptot_for_year(self, year: int, dataset: xr.Dataset, percentile_95: np.ndarray) -> Optional[np.ndarray]:
        """
        Calculate R95pTOT values for a specific year using the base period percentile.
        
        Args:
            year: Year to calculate R95pTOT for
            dataset: xarray Dataset with daily precipitation data
            percentile_95: 2D array with 95th percentile values for each pixel
            
        Returns:
            numpy array with R95pTOT values, or None if calculation fails
        """
        try:
            info("Calculating R95pTOT for year",
                 component="r95ptot_calculator",
                 year=year,
                 dataset_shape=dataset['Precipitation'].shape)
            
            # Get precipitation data
            precip_data = dataset['Precipitation']
            precip_values = precip_data.values
            
            # Handle invalid values (CHIRPS often uses -9999 for no data)
            invalid_mask = (precip_values < 0) | (precip_values == -9999) | (precip_values > 1000)
            if np.any(invalid_mask):
                precip_values = precip_values.copy()
                precip_values[invalid_mask] = np.nan
                invalid_count = np.sum(invalid_mask)
                info(f"Converted {invalid_count} invalid values to NaN",
                     component="r95ptot_calculator",
                     year=year,
                     invalid_count=invalid_count)
            
            # Check if values are in m/day and convert to mm/day
            valid_values = precip_values[~np.isnan(precip_values)]
            if len(valid_values) > 0 and np.max(valid_values) < 1 and np.max(valid_values) > 0:
                precip_values = precip_values * 1000
                info("Converted precipitation from m to mm",
                     component="r95ptot_calculator",
                     year=year)
            
            # Calculate R95pTOT for each pixel
            r95ptot_values = self._calculate_extreme_precipitation_total(precip_values, percentile_95)
            
            # Handle cases where all values were NaN
            all_nan_mask = np.all(np.isnan(precip_values), axis=0)
            r95ptot_values[all_nan_mask] = np.nan
            
            info("R95pTOT calculation completed for year",
                 component="r95ptot_calculator",
                 year=year,
                 max_r95ptot=np.nanmax(r95ptot_values),
                 min_r95ptot=np.nanmin(r95ptot_values),
                 mean_r95ptot=np.nanmean(r95ptot_values))
            
            return r95ptot_values
            
        except Exception as e:
            error("Failed to calculate R95pTOT for year",
                  component="r95ptot_calculator",
                  year=year,
                  error=str(e))
            return None

    def _calculate_extreme_precipitation_total(self, precip_values: np.ndarray, percentile_95: np.ndarray) -> np.ndarray:
        """
        Calculate total precipitation on days above 95th percentile for each pixel.
        
        Args:
            precip_values: 3D array (time, lat, lon) with precipitation values
            percentile_95: 2D array (lat, lon) with 95th percentile values
            
        Returns:
            2D array (lat, lon) with total extreme precipitation values
        """
        time_steps, height, width = precip_values.shape
        r95ptot_result = np.zeros((height, width), dtype=np.float32)
        
        # Process each pixel
        for i in range(height):
            for j in range(width):
                pixel_precip = precip_values[:, i, j]
                pixel_percentile = percentile_95[i, j]
                
                # Skip if percentile is NaN or all values are NaN
                if np.isnan(pixel_percentile) or np.all(np.isnan(pixel_precip)):
                    r95ptot_result[i, j] = np.nan
                    continue
                
                # Count valid precipitation values
                valid_precip = pixel_precip[~np.isnan(pixel_precip)]
                
                if len(valid_precip) == 0:
                    r95ptot_result[i, j] = np.nan
                    continue
                
                # Sum precipitation on days above 95th percentile
                extreme_days_mask = valid_precip > pixel_percentile
                extreme_precip = valid_precip[extreme_days_mask]
                
                # Total extreme precipitation
                if len(extreme_precip) > 0:
                    r95ptot_result[i, j] = np.sum(extreme_precip)
                else:
                    r95ptot_result[i, j] = 0.0
        
        return r95ptot_result

    def _save_r95ptot_results(self, results: dict, datasets: dict) -> bool:
        """
        Save R95pTOT calculation results to GeoTIFF files.
        
        Args:
            results: Dictionary mapping years to R95pTOT arrays
            datasets: Dictionary mapping years to xarray Datasets (for spatial info)
            
        Returns:
            bool: True if saving was successful
        """
        try:
            info("Saving R95pTOT results",
                 component="r95ptot_calculator",
                 year_count=len(results))
            
            # For each year, save as GeoTIFF
            for year, r95ptot_data in results.items():
                output_filename = self._generate_climate_index_filename(year)
                output_path = self.output_path / output_filename
                
                # Save as GeoTIFF using spatial info from dataset
                dataset = datasets[year]
                self._save_as_geotiff(r95ptot_data, output_path, year, dataset)
                
                info("R95pTOT result saved",
                     component="r95ptot_calculator",
                     year=year,
                     output_file=str(output_path))
            
            return True
            
        except Exception as e:
            error("Failed to save R95pTOT results",
                  component="r95ptot_calculator",
                  error=str(e))
            return False

    def _save_as_geotiff(self, data: np.ndarray, output_path: Path, year: int, dataset: xr.Dataset):
        """
        Save data as GeoTIFF with proper georeferencing from dataset.
        
        Args:
            data: 2D numpy array with R95pTOT values
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
                    INDICATOR='R95pTOT',
                    YEAR=str(year),
                    DESCRIPTION=f'Total precipitation on days > 95th percentile of wet days in base period ({self.base_period_start}-{self.base_period_end})',
                    UNITS='mm',
                    BASE_PERIOD=f'{self.base_period_start}-{self.base_period_end}',
                    CREATED=datetime.now().isoformat()
                )
                
            info("R95pTOT data saved as GeoTIFF",
                 component="r95ptot_calculator",
                 output_path=str(output_path),
                 year=year,
                 shape=data.shape)
                
        except Exception as e:
            error("Failed to save R95pTOT data as GeoTIFF",
                  component="r95ptot_calculator",
                  output_path=str(output_path),
                  year=year,
                  error=str(e))
            raise

    def calculate_monthly(self) -> bool:
        """
        Monthly R95pTOT calculation (not implemented yet).

        This would calculate the total extreme precipitation for each month.
        """
        warning("Monthly R95pTOT calculation not implemented",
               component="r95ptot_calculator",
               indicator_code=self.INDICATOR_CODE)
        return False


class R95pTOTDataProcessor:
    """
    Helper class for R95pTOT data processing operations.
    
    This class could contain methods for:
    - Loading daily precipitation data from various sources
    - Calculating percentiles for base periods (wet days only)
    - Data quality checks and filtering
    - Coordinate system transformations
    - Metadata handling
    """
    
    @staticmethod
    def calculate_wet_day_percentile(daily_data: xr.Dataset, percentile: float = 95.0, threshold: float = 1.0) -> xr.Dataset:
        """
        Calculate percentile for wet days in base period daily precipitation data.
        
        Args:
            daily_data: Daily precipitation dataset for base period
            percentile: Percentile value to calculate (default 95.0)
            threshold: Wet day threshold in mm (default 1.0 mm)
            
        Returns:
            xr.Dataset: Percentile values for each grid point
        """
        # TODO: Implement wet day percentile calculation
        # This would calculate the specified percentile for each grid point
        # considering only wet days (precipitation >= threshold)
        pass
    
    @staticmethod
    def calculate_extreme_precipitation_totals(daily_data: xr.Dataset, thresholds: xr.Dataset) -> xr.Dataset:
        """
        Calculate total precipitation on days above threshold for each year.
        
        Args:
            daily_data: Daily precipitation dataset
            thresholds: Threshold values for each grid point
            
        Returns:
            xr.Dataset: Annual total extreme precipitation values
        """
        # TODO: Implement extreme precipitation totals calculation
        # This would:
        # 1. Identify days above threshold for each grid point
        # 2. Sum precipitation on these extreme days for each year
        # 3. Return annual totals
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