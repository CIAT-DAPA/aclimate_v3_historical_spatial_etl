import os
import xarray as xr
import numpy as np
from pathlib import Path
from typing import Optional, Dict, Tuple
from datetime import datetime
from abc import ABC, abstractmethod
from .base_calculator import BaseIndicatorCalculator
from .data_downloader import IndicatorDataDownloader
from ...tools import info, error, warning


class PercentileBasedCalculator(BaseIndicatorCalculator, ABC):
    """
    Base class for percentile-based climate indicators.
    
    This class handles the common functionality for indicators that require
    calculating percentiles from a base period (like TX90p, TX10p, R95pTOT).
    
    It provides:
    - Centralized base period configuration per data type
    - Shared percentile calculation and caching
    - Common data processing methods
    """
    
    # Data-specific base period configuration
    # Different data sources have different availability periods
    BASE_PERIODS = {
        "temperature": {"start": "1981", "end": "2010"},  # ERA5 reliable from 1979
        "precipitation": {"start": "1981", "end": "2010"}  # CHIRPS available from 1981
    }
    
    # Class-level cache for percentiles to avoid recalculation
    _percentile_cache = {}
    
    # Class-level cache for base period datasets to enable data reuse
    _base_period_data_cache = {}
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache_key = None
    
    @property
    @abstractmethod
    def required_percentiles(self) -> list:
        """
        List of percentiles required by this indicator.
        Should be overridden by subclasses.
        
        Returns:
            List of percentile values (e.g., [10, 90, 95])
        """
        pass
    
    @property
    @abstractmethod
    def data_type(self) -> str:
        """
        Type of data for base period selection.
        Should be overridden by subclasses.
        
        Returns:
            Data type ('temperature' or 'precipitation')
        """
        pass
    
    @property
    @abstractmethod
    def data_variable(self) -> str:
        """
        Name of the data variable to use for percentile calculation.
        Should be overridden by subclasses.
        
        Returns:
            Variable name (e.g., "2m_Maximum_Temperature", "Precipitation")
        """
        pass
    
    @property
    @abstractmethod
    def geoserver_layer_suffix(self) -> str:
        """
        Suffix for the GeoServer layer name.
        Should be overridden by subclasses.
        
        Returns:
            Layer suffix (e.g., "tmax", "prec")
        """
        pass
    
    @property
    def base_period_start(self) -> str:
        """Get the start year for the base period based on data type."""
        return self.BASE_PERIODS[self.data_type]["start"]
    
    @property
    def base_period_end(self) -> str:
        """Get the end year for the base period based on data type."""
        return self.BASE_PERIODS[self.data_type]["end"]
    
    def _get_cache_key(self) -> str:
        """Generate a unique cache key for this calculator's percentiles."""
        if self._cache_key is None:
            percentiles_str = "_".join(map(str, sorted(self.required_percentiles)))
            self._cache_key = f"{self.country_code}_{self.data_variable}_{percentiles_str}_{self.base_period_start}_{self.base_period_end}"
        return self._cache_key
    
    def _get_geoserver_config(self) -> dict:
        """Get GeoServer configuration for the data variable."""
        try:
            workspace = f"climate_historical_daily"
            layer = f"climate_historical_daily_{self.country_code}_{self.geoserver_layer_suffix}"
            store = f"climate_historical_daily_{self.country_code}_{self.geoserver_layer_suffix}"
            
            return {
                'workspace': workspace,
                'layer': layer,
                'store': store
            }
        except Exception as e:
            error("Failed to get GeoServer configuration",
                  component=f"{self.INDICATOR_CODE.lower()}_calculator",
                  error=str(e))
            return {}
    
    def _get_base_data_cache_key(self) -> str:
        """Generate cache key for base period datasets."""
        return f"{self.country_code}_{self.data_variable}_{self.base_period_start}_{self.base_period_end}"
    
    def _group_consecutive_years(self, years: list) -> list:
        """
        Group consecutive years into ranges for efficient downloading.
        
        Args:
            years: Sorted list of years
            
        Returns:
            List of tuples (start_year, end_year) for consecutive ranges
        """
        if not years:
            return []
        
        ranges = []
        start = years[0]
        end = years[0]
        
        for year in years[1:]:
            if year == end + 1:
                end = year
            else:
                ranges.append((start, end))
                start = end = year
        
        ranges.append((start, end))
        return ranges
    
    def get_datasets_for_indicator_calculation(self, start_year: str, end_year: str) -> Optional[Dict[int, any]]:
        """
        Get datasets for indicator calculation, reusing base period data when possible.
        
        This method intelligently downloads only the data that's not already available
        from the base period calculation, avoiding duplicate downloads.
        
        Args:
            start_year: Start year for indicator calculation
            end_year: End year for indicator calculation
            
        Returns:
            Dictionary mapping years to datasets, or None if download fails
        """
        try:
            start_year_int = int(start_year)
            end_year_int = int(end_year)
            base_start = int(self.base_period_start)
            base_end = int(self.base_period_end)
            
            info(f"Getting datasets for {self.INDICATOR_CODE} calculation",
                 component=f"{self.INDICATOR_CODE.lower()}_calculator",
                 indicator_period=f"{start_year}-{end_year}",
                 base_period=f"{self.base_period_start}-{self.base_period_end}",
                 data_type=self.data_type)
            
            # Check if we already have base period data cached
            base_cache_key = self._get_base_data_cache_key()
            base_period_data = self._base_period_data_cache.get(base_cache_key)
            
            # Determine which years we need to download
            all_needed_years = set(range(start_year_int, end_year_int + 1))
            available_from_base = set()
            
            if base_period_data:
                # We have base period data, check overlap
                base_years = set(range(base_start, base_end + 1))
                available_from_base = all_needed_years.intersection(base_years)
                info(f"Found {len(available_from_base)} years available from base period cache",
                     component=f"{self.INDICATOR_CODE.lower()}_calculator",
                     available_years=sorted(available_from_base))
            
            # Years we still need to download
            years_to_download = all_needed_years - available_from_base
            
            # Start with data from base period cache if available
            all_datasets = {}
            if available_from_base:
                for year in available_from_base:
                    if year in base_period_data:
                        all_datasets[year] = base_period_data[year]
                        info(f"Reusing base period data for year {year}",
                             component=f"{self.INDICATOR_CODE.lower()}_calculator")
            
            # Download additional years if needed
            if years_to_download:
                info(f"Downloading {len(years_to_download)} additional years",
                     component=f"{self.INDICATOR_CODE.lower()}_calculator",
                     years_to_download=sorted(years_to_download))
                
                # Group consecutive years for efficient downloading
                year_ranges = self._group_consecutive_years(sorted(years_to_download))
                
                geoserver_config = self._get_geoserver_config()
                for range_start, range_end in year_ranges:
                    downloader = IndicatorDataDownloader(
                        geoserver_workspace=geoserver_config['workspace'],
                        geoserver_layer=geoserver_config['layer'],
                        output_path=self.output_path / "temp_indicator_downloads",
                        variable=self.data_variable,
                        year_range=(range_start, range_end),
                        parallel_downloads=4
                    )
                    
                    range_datasets = downloader.download_all_years()
                    if range_datasets:
                        all_datasets.update(range_datasets)
                    else:
                        error(f"Failed to download years {range_start}-{range_end}",
                              component=f"{self.INDICATOR_CODE.lower()}_calculator")
                        return None
            
            if len(all_datasets) != len(all_needed_years):
                missing_years = all_needed_years - set(all_datasets.keys())
                error(f"Missing data for years: {sorted(missing_years)}",
                      component=f"{self.INDICATOR_CODE.lower()}_calculator")
                return None
            
            info(f"Successfully obtained datasets for all {len(all_datasets)} years",
                 component=f"{self.INDICATOR_CODE.lower()}_calculator",
                 reused_count=len(available_from_base),
                 downloaded_count=len(years_to_download))
            
            return all_datasets
            
        except Exception as e:
            error(f"Failed to get datasets for indicator calculation: {str(e)}",
                  component=f"{self.INDICATOR_CODE.lower()}_calculator")
            return None

    def get_base_period_percentiles(self) -> Optional[Dict[int, np.ndarray]]:
        """
        Get or calculate base period percentiles for this indicator.
        Uses caching to avoid recalculation across multiple indicators.
        
        Returns:
            Dictionary mapping percentile values to 2D arrays, or None if calculation fails
        """
        cache_key = self._get_cache_key()
        
        # Check if percentiles are already cached
        if cache_key in self._percentile_cache:
            info(f"Using cached percentiles for {self.INDICATOR_CODE}",
                 component=f"{self.INDICATOR_CODE.lower()}_calculator",
                 cache_key=cache_key)
            return self._percentile_cache[cache_key]
        
        # Calculate percentiles
        info(f"Calculating base period percentiles for {self.INDICATOR_CODE}",
             component=f"{self.INDICATOR_CODE.lower()}_calculator",
             percentiles=self.required_percentiles,
             data_type=self.data_type,
             base_period=f"{self.base_period_start}-{self.base_period_end}")
        
        percentiles_dict = self._calculate_base_period_percentiles()
        
        if percentiles_dict is not None:
            # Cache the results
            self._percentile_cache[cache_key] = percentiles_dict
            info(f"Cached percentiles for future use",
                 component=f"{self.INDICATOR_CODE.lower()}_calculator",
                 cache_key=cache_key)
        
        return percentiles_dict
    
    def _calculate_base_period_percentiles(self) -> Optional[Dict[int, np.ndarray]]:
        """
        Calculate percentiles from the base period data.
        
        Returns:
            Dictionary mapping percentile values to 2D arrays, or None if calculation fails
        """
        try:
            info("Downloading base period data for percentile calculation",
                 component=f"{self.INDICATOR_CODE.lower()}_calculator",
                 data_type=self.data_type,
                 base_period=f"{self.base_period_start}-{self.base_period_end}",
                 data_variable=self.data_variable)
            
            geoserver_config = self._get_geoserver_config()
            downloader = IndicatorDataDownloader(
                geoserver_workspace=geoserver_config['workspace'],
                geoserver_layer=geoserver_config['layer'],
                output_path=self.output_path / "temp_base_period",
                variable=self.data_variable,
                year_range=(int(self.base_period_start), int(self.base_period_end)),
                parallel_downloads=4
            )
            
            # Download all base period data
            base_datasets = downloader.download_all_years()
            
            if not base_datasets:
                error("Failed to download base period data",
                      component=f"{self.INDICATOR_CODE.lower()}_calculator")
                return None
            
            # Cache the base period datasets for reuse in indicator calculations
            base_cache_key = self._get_base_data_cache_key()
            self._base_period_data_cache[base_cache_key] = base_datasets
            info(f"Cached base period datasets for reuse",
                 component=f"{self.INDICATOR_CODE.lower()}_calculator",
                 cache_key=base_cache_key,
                 years_cached=sorted(base_datasets.keys()))
            
            # Combine all years into a single time series
            all_data = []
            for year, dataset in sorted(base_datasets.items()):
                data_values = dataset[self.data_variable].values
                
                # Apply data-specific preprocessing
                data_values = self._preprocess_data(data_values, year)
                all_data.append(data_values)
            
            # Concatenate all years along time axis
            all_data_combined = np.concatenate(all_data, axis=0)
            
            # Calculate requested percentiles
            percentiles_dict = {}
            for percentile in self.required_percentiles:
                info(f"Calculating {percentile}th percentile",
                     component=f"{self.INDICATOR_CODE.lower()}_calculator",
                     percentile=percentile)
                
                percentile_values = self._calculate_percentile_for_variable(
                    all_data_combined, percentile
                )
                
                if percentile_values is not None:
                    percentiles_dict[percentile] = percentile_values
                    
                    info(f"{percentile}th percentile calculated successfully",
                         component=f"{self.INDICATOR_CODE.lower()}_calculator",
                         percentile=percentile,
                         shape=percentile_values.shape,
                         min_val=np.nanmin(percentile_values),
                         max_val=np.nanmax(percentile_values),
                         mean_val=np.nanmean(percentile_values))
                else:
                    error(f"Failed to calculate {percentile}th percentile",
                          component=f"{self.INDICATOR_CODE.lower()}_calculator",
                          percentile=percentile)
                    return None
            
            # Clean up base period download directory
            try:
                temp_base_dir = self.output_path / "temp_base_period"
                if temp_base_dir.exists():
                    import shutil
                    shutil.rmtree(temp_base_dir)
                    info("Base period temporary directory cleaned up",
                         component=f"{self.INDICATOR_CODE.lower()}_calculator")
            except Exception as e:
                warning("Failed to clean up base period directory",
                        component=f"{self.INDICATOR_CODE.lower()}_calculator",
                        error=str(e))
            
            return percentiles_dict
            
        except Exception as e:
            error("Failed to calculate base period percentiles",
                  component=f"{self.INDICATOR_CODE.lower()}_calculator",
                  error=str(e))
            return None
    
    @abstractmethod
    def _preprocess_data(self, data_values: np.ndarray, year: int) -> np.ndarray:
        """
        Preprocess data values before percentile calculation.
        Should be overridden by subclasses for variable-specific processing.
        
        Args:
            data_values: Raw data values from dataset
            year: Year being processed
            
        Returns:
            Preprocessed data values
        """
        pass
    
    @abstractmethod
    def _calculate_percentile_for_variable(self, data_combined: np.ndarray, percentile: int) -> Optional[np.ndarray]:
        """
        Calculate percentile for the specific variable type.
        Should be overridden by subclasses for variable-specific percentile calculation.
        
        Args:
            data_combined: Combined data from all base period years
            percentile: Percentile value to calculate
            
        Returns:
            2D array with percentile values for each pixel, or None if calculation fails
        """
        pass
    
    @classmethod
    def clear_percentile_cache(cls):
        """Clear the percentile cache. Useful for testing or memory management."""
        cls._percentile_cache.clear()
        cls._base_period_data_cache.clear()
        info("Percentile and base period data caches cleared", component="percentile_cache")
    
    @classmethod
    def get_cache_info(cls) -> dict:
        """Get information about cached percentiles."""
        return {
            'percentile_cache_keys': list(cls._percentile_cache.keys()),
            'percentile_cache_size': len(cls._percentile_cache),
            'base_data_cache_keys': list(cls._base_period_data_cache.keys()),
            'base_data_cache_size': len(cls._base_period_data_cache),
            'memory_usage_mb': sum(
                sum(arr.nbytes for arr in percentiles_dict.values()) / (1024 * 1024)
                for percentiles_dict in cls._percentile_cache.values()
            ) if cls._percentile_cache else 0
        }


class TemperaturePercentileCalculator(PercentileBasedCalculator):
    """
    Base class for temperature-based percentile indicators (TX90p, TX10p).
    """
    
    @property
    def data_type(self) -> str:
        return "temperature"
    
    @property
    def data_variable(self) -> str:
        return "2m_Maximum_Temperature"
    
    @property
    def geoserver_layer_suffix(self) -> str:
        return "tmax"
    
    def _preprocess_data(self, data_values: np.ndarray, year: int) -> np.ndarray:
        """Preprocess temperature data: convert from Kelvin to Celsius if needed."""
        # Convert temperature to Celsius if needed (check if in Kelvin range)
        if np.nanmean(data_values) > 200:
            data_values = data_values - 273.15
            info("Converted temperature from Kelvin to Celsius",
                 component=f"{self.INDICATOR_CODE.lower()}_calculator",
                 year=year)
        
        return data_values
    
    def _calculate_percentile_for_variable(self, data_combined: np.ndarray, percentile: int) -> Optional[np.ndarray]:
        """Calculate percentile for temperature data (simple percentile across all values)."""
        try:
            # Simple percentile calculation for temperature
            percentile_values = np.nanpercentile(data_combined, percentile, axis=0)
            return percentile_values
        except Exception as e:
            error(f"Failed to calculate temperature percentile",
                  component=f"{self.INDICATOR_CODE.lower()}_calculator",
                  percentile=percentile,
                  error=str(e))
            return None


class PrecipitationPercentileCalculator(PercentileBasedCalculator):
    """
    Base class for precipitation-based percentile indicators (R95pTOT).
    """
    
    @property
    def data_type(self) -> str:
        return "precipitation"
    
    @property
    def data_variable(self) -> str:
        return "Precipitation"
    
    @property
    def geoserver_layer_suffix(self) -> str:
        return "prec"
    
    def _preprocess_data(self, data_values: np.ndarray, year: int) -> np.ndarray:
        """Preprocess precipitation data: handle invalid values and unit conversion."""
        # Handle invalid values (CHIRPS often uses -9999 for no data)
        invalid_mask = (data_values < 0) | (data_values == -9999) | (data_values > 1000)
        if np.any(invalid_mask):
            data_values = data_values.copy()
            data_values[invalid_mask] = np.nan
            invalid_count = np.sum(invalid_mask)
            info(f"Converted {invalid_count} invalid values to NaN",
                 component=f"{self.INDICATOR_CODE.lower()}_calculator",
                 year=year,
                 invalid_count=invalid_count)
        
        # Check if values are in m/day and convert to mm/day
        valid_values = data_values[~np.isnan(data_values)]
        if len(valid_values) > 0 and np.max(valid_values) < 1 and np.max(valid_values) > 0:
            data_values = data_values * 1000
            info("Converted precipitation from m to mm",
                 component=f"{self.INDICATOR_CODE.lower()}_calculator",
                 year=year)
        
        return data_values
    
    def _calculate_percentile_for_variable(self, data_combined: np.ndarray, percentile: int) -> Optional[np.ndarray]:
        """Calculate percentile for precipitation data (only considering wet days >= 1 mm)."""
        try:
            time_steps, height, width = data_combined.shape
            percentile_values = np.zeros((height, width), dtype=np.float32)
            
            # Calculate percentile pixel by pixel, considering only wet days
            for i in range(height):
                for j in range(width):
                    pixel_data = data_combined[:, i, j]
                    
                    # Filter out NaN values and only consider wet days (>= 1 mm)
                    valid_data = pixel_data[~np.isnan(pixel_data)]
                    wet_days = valid_data[valid_data >= 1.0]
                    
                    if len(wet_days) > 0:
                        percentile_values[i, j] = np.percentile(wet_days, percentile)
                    else:
                        percentile_values[i, j] = np.nan
            
            return percentile_values
            
        except Exception as e:
            error(f"Failed to calculate precipitation percentile",
                  component=f"{self.INDICATOR_CODE.lower()}_calculator",
                  percentile=percentile,
                  error=str(e))
            return None