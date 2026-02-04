import os
import xarray as xr
import numpy as np
import rasterio
from pathlib import Path
from typing import Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..base_calculator import BaseIndicatorCalculator
from ..data_downloader import IndicatorDataDownloader
from ....tools import info, error, warning


class TR20Calculator(BaseIndicatorCalculator):
    """
    Calculator for TR20 indicator: Annual count of days with minimum temperature above 20°C.
    
    TR20 represents the number of days in a year where the daily minimum temperature (Tmin)
    exceeds 20°C. This indicator shows the frequency of warm nights annually.
    """

    INDICATOR_CODE = "TR20"
    SUPPORTED_TEMPORALITIES = ["annual"]  # Only annual for now
    
    def calculate_annual(self) -> bool:
        """
        Calculate annual TR20 values.
        
        This method calculates the number of days with minimum temperature above 20°C
        for each year in the specified date range.
        
        Returns:
            bool: True if calculation was successful, False otherwise
        """
        try:
            info("Starting TR20 annual calculation",
                 component="tr20_calculator",
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
                variable="2m_Minimum_Temperature",
                year_range=(start_year, end_year),
                parallel_downloads=4
            )
            
            # Download data for all years
            datasets = downloader.download_all_years()
            
            if not datasets:
                error("No data downloaded",
                      component="tr20_calculator")
                return False
            
            # Calculate TR20 for each year in parallel
            results = {}
            max_workers = int(os.getenv('MAX_PARALLEL_DOWNLOADS', 4))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_year = {
                    executor.submit(self._calculate_tr20_for_year, year, dataset): year
                    for year, dataset in datasets.items()
                }
                
                for future in as_completed(future_to_year):
                    year = future_to_year[future]
                    try:
                        result = future.result()
                        if result is not None:
                            results[year] = result
                            info("TR20 calculated for year",
                                 component="tr20_calculator",
                                 year=year)
                    except Exception as e:
                        error("Failed to calculate TR20 for year",
                              component="tr20_calculator",
                              year=year,
                              error=str(e))
            
            # Save results
            if results:
                success = self._save_tr20_results(results, datasets)
                
                # Clean up temporary download directory
                try:
                    temp_dir = self.output_path / "temp_downloads"
                    if temp_dir.exists():
                        import shutil
                        shutil.rmtree(temp_dir)
                        info("Temporary download directory cleaned up",
                             component="tr20_calculator",
                             temp_dir=str(temp_dir))
                except Exception as e:
                    warning("Failed to clean up temporary directory",
                            component="tr20_calculator",
                            temp_dir=str(temp_dir),
                            error=str(e))
                
                return success
            else:
                error("No TR20 results calculated",
                      component="tr20_calculator")
                return False
                
        except Exception as e:
            error("Failed to calculate TR20 annual values",
                  component="tr20_calculator",
                  indicator_code=self.INDICATOR_CODE,
                  error=str(e))
            
            # Clean up temporary download directory even on error
            try:
                temp_dir = self.output_path / "temp_downloads"
                if temp_dir.exists():
                    import shutil
                    shutil.rmtree(temp_dir)
                    info("Temporary download directory cleaned up after error",
                         component="tr20_calculator",
                         temp_dir=str(temp_dir))
            except Exception as cleanup_error:
                warning("Failed to clean up temporary directory after error",
                        component="tr20_calculator",
                        temp_dir=str(temp_dir),
                        error=str(cleanup_error))
            
            return False

    def _get_geoserver_config(self) -> dict:
        """Get GeoServer configuration for temperature data"""
        try:
            # This should be provided through the config or determined based on country
            # For now, we'll use a basic structure
            workspace = f"climate_historical_daily"
            layer = f"climate_historical_daily_{self.country_code}_tmin"
            store = f"climate_historical_daily_{self.country_code}_tmin"
            
            return {
                'workspace': workspace,
                'layer': layer,
                'store': store
            }
        except Exception as e:
            error("Failed to get GeoServer configuration",
                  component="tr20_calculator",
                  error=str(e))
            return {}

    def _calculate_tr20_for_year(self, year: int, dataset: xr.Dataset) -> Optional[np.ndarray]:
        """
        Calculate TR20 values for a specific year using xarray Dataset.
        
        Args:
            year: Year to calculate TR20 for
            dataset: xarray Dataset with daily minimum temperature data
            
        Returns:
            numpy array with TR20 values, or None if calculation fails
        """
        try:
            info("Calculating TR20 for year",
                 component="tr20_calculator",
                 year=year,
                 dataset_shape=dataset['2m_Minimum_Temperature'].shape)
            
            # Get temperature data
            temp_data = dataset['2m_Minimum_Temperature']
            
            # Convert temperature to Celsius if needed (assuming data is in Kelvin)
            # Check if temperature values are in Kelvin range (typically > 200)
            temp_values = temp_data.values
            if np.nanmean(temp_values) > 200:
                temp_values = temp_values - 273.15  # Convert from Kelvin to Celsius
                info("Converted temperature from Kelvin to Celsius",
                     component="tr20_calculator",
                     year=year)
            
            # Count days where minimum temperature > 20°C for each pixel
            hot_days_mask = temp_values > 20.0
            
            # Count days per pixel (sum along time axis)
            tr20_values = np.sum(hot_days_mask, axis=0)
            
            # Handle NaN values - set to NaN where all values were NaN
            all_nan_mask = np.all(np.isnan(temp_data), axis=0)
            tr20_values = tr20_values.astype(float)
            tr20_values[all_nan_mask] = np.nan
            
            info("TR20 calculation completed for year",
                 component="tr20_calculator",
                 year=year,
                 max_tr20=np.nanmax(tr20_values),
                 min_tr20=np.nanmin(tr20_values),
                 mean_tr20=np.nanmean(tr20_values))
            
            return tr20_values
            
        except Exception as e:
            error("Failed to calculate TR20 for year",
                  component="tr20_calculator",
                  year=year,
                  error=str(e))
            return None

    def _save_tr20_results(self, results: dict, datasets: dict) -> bool:
        """
        Save TR20 calculation results to GeoTIFF files.
        
        Args:
            results: Dictionary mapping years to TR20 arrays
            
        Returns:
            bool: True if saving was successful
        """
        try:
            info("Saving TR20 results",
                 component="tr20_calculator",
                 year_count=len(results))
            
            # Get reference file for spatial metadata
            sample_year = list(results.keys())[0]
            sample_data = results[sample_year]
            
            # For each year, save as GeoTIFF
            for year, tr20_data in results.items():
                output_filename = self._generate_climate_index_filename(year)
                output_path = self.output_path / output_filename
                
                # Save as GeoTIFF using spatial info from dataset
                dataset = datasets[year]
                self._save_as_geotiff(tr20_data, output_path, year, dataset)
                
                info("TR20 result saved",
                     component="tr20_calculator",
                     year=year,
                     output_file=str(output_path))
            
            return True
            
        except Exception as e:
            error("Failed to save TR20 results",
                  component="tr20_calculator",
                  error=str(e))
            return False

    def _save_as_geotiff(self, data: np.ndarray, output_path: Path, year: int, dataset: xr.Dataset):
        """
        Save data as GeoTIFF with proper georeferencing from dataset.
        
        Args:
            data: 2D numpy array with TR20 values
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
                    INDICATOR='TR20',
                    YEAR=str(year),
                    DESCRIPTION='Annual count of days with minimum temperature above 20°C',
                    UNITS='days',
                    CREATED=datetime.now().isoformat()
                )
                
            info("TR20 data saved as GeoTIFF",
                 component="tr20_calculator",
                 output_path=str(output_path),
                 year=year,
                 shape=data.shape)
                
        except Exception as e:
            error("Failed to save TR20 data as GeoTIFF",
                  component="tr20_calculator",
                  output_path=str(output_path),
                  year=year,
                  error=str(e))
            raise

    def _placeholder_calculation(self, output_path: Path) -> bool:
        """
        Placeholder calculation method to demonstrate the structure.
        
        In a real implementation, this would be replaced with actual
        temperature data processing using xarray, rasterio, etc.
        
        Args:
            output_path: Path where the result should be saved
            
        Returns:
            bool: True if successful
        """
        try:
            # Create a simple placeholder file to show the structure works
            placeholder_content = f"""# TXx Calculation Result - PLACEHOLDER
# Indicator: {self.indicator_name}
# Code: {self.INDICATOR_CODE}
# Temporality: {self.temporality}
# Country: {self.country_code.upper()}
# Date Range: {self.start_date} to {self.end_date}
# Unit: {self.unit}
# Output: {output_path}
# Timestamp: {datetime.now().isoformat()}

# This is a placeholder file. In a real implementation, this would be:
# - A GeoTIFF file with calculated TXx values
# - NetCDF file with annual maximum temperatures
# - Processed raster data ready for GeoServer upload

# Real implementation would:
# 1. Load daily Tmax data for the date range
# 2. Calculate annual maximum for each pixel/location
# 3. Generate appropriate output format (TIFF/NetCDF)
# 4. Include proper metadata and coordinate reference system
"""
            
            # Write placeholder content
            with open(output_path.with_suffix('.txt'), 'w') as f:
                f.write(placeholder_content)

            info("Placeholder TR20 file created",
                 component="tr20_calculator",
                 file=str(output_path.with_suffix('.txt')))
            
            return True
            
        except Exception as e:
            error("Failed to create placeholder file",
                  component="tr20_calculator",
                  error=str(e))
            return False

    def calculate_monthly(self) -> bool:
        """
        Monthly TR20 calculation (not implemented yet).

        This would calculate the number of days with minimum temperature above 20°C for each month.
        """
        warning("Monthly TR20 calculation not implemented",
               component="tr20_calculator",
               indicator_code=self.INDICATOR_CODE)
        return False


# Additional methods that could be added for a complete TR20 implementation:

class TR20DataProcessor:
    """
    Helper class for TR20 data processing operations.
    
    This class could contain methods for:
    - Loading daily temperature data from various sources
    - Data quality checks and filtering
    - Coordinate system transformations
    - Metadata handling
    """
    
    @staticmethod
    def load_daily_tmax_data(data_path: Path, start_date: str, end_date: str) -> xr.Dataset:
        """
        Load daily maximum temperature data for the specified period.
        
        Args:
            data_path: Path to temperature data files
            start_date: Start date in YYYY-MM format
            end_date: End date in YYYY-MM format
            
        Returns:
            xr.Dataset: Daily temperature data
        """
        # TODO: Implement data loading logic
        # This could load from:
        # - NetCDF files
        # - TIFF files
        # - Database queries
        # - GeoServer WCS requests
        pass
    
    @staticmethod
    def calculate_annual_maximum(daily_data: xr.Dataset) -> xr.Dataset:
        """
        Calculate annual maximum from daily data.
        
        Args:
            daily_data: Daily temperature dataset
            
        Returns:
            xr.Dataset: Annual maximum values
        """
        # TODO: Implement annual maximum calculation
        # Example with xarray:
        # return daily_data.groupby('time.year').max('time')
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