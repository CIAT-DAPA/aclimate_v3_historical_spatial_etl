from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
from ...tools import info, error, warning


class BaseIndicatorCalculator(ABC):
    """
    Abstract base class for all climate indicator calculators.
    
    Each indicator calculator must implement this interface and define:
    - INDICATOR_CODE: Short name of the indicator (e.g., "TXX")
    - SUPPORTED_TEMPORALITIES: List of supported time periods
    - calculate_* methods for each supported temporality
    """
    
    # Must be overridden in subclasses
    INDICATOR_CODE: str = None
    SUPPORTED_TEMPORALITIES: List[str] = []
    
    def __init__(
        self,
        indicator_config: Dict[str, Any],
        output_path: Path,
        start_date: str,
        end_date: str,
        country_code: str,
        naming_config: Dict[str, Any]
    ):
        """
        Initialize the calculator.
        
        Args:
            indicator_config: Full indicator configuration from database
            output_path: Directory to save calculated results
            start_date: Start date in YYYY-MM format
            end_date: End date in YYYY-MM format
            country_code: ISO2 country code (e.g., "hn")
            naming_config: Naming conventions configuration
        """
        self.config = indicator_config
        self.output_path = Path(output_path)
        self.start_date = start_date
        self.end_date = end_date
        self.country_code = country_code.lower()
        self.naming_config = naming_config
        
        # Extract indicator details
        self.indicator_name = indicator_config.get('name', 'Unknown')
        self.short_name = indicator_config.get('short_name', 'UNKNOWN')
        self.temporality = indicator_config.get('temporality', 'annual')
        self.unit = indicator_config.get('unit', '')
        self.country_config = indicator_config.get('country_config', {})
        
        # Ensure output directory exists
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        info("Indicator calculator initialized",
             component="indicator_calculation",
             indicator_code=self.INDICATOR_CODE,
             indicator_name=self.indicator_name,
             temporality=self.temporality,
             output_path=str(self.output_path))

    def calculate(self) -> bool:
        """
        Main calculation method that routes to the appropriate temporality method.
        
        Returns:
            bool: True if calculation was successful, False otherwise
        """
        try:
            info("Starting indicator calculation",
                 component="indicator_calculation",
                 indicator_code=self.INDICATOR_CODE,
                 temporality=self.temporality)
            
            # Validate temporality is supported
            if self.temporality not in self.SUPPORTED_TEMPORALITIES:
                error(f"Temporality '{self.temporality}' not supported",
                      component="indicator_calculation",
                      indicator_code=self.INDICATOR_CODE,
                      supported=self.SUPPORTED_TEMPORALITIES)
                return False
            
            # Route to appropriate calculation method
            method_name = f"calculate_{self.temporality}"
            if not hasattr(self, method_name):
                error(f"Method '{method_name}' not implemented",
                      component="indicator_calculation",
                      indicator_code=self.INDICATOR_CODE)
                return False
            
            method = getattr(self, method_name)
            result = method()
            
            if result:
                info("Indicator calculation completed successfully",
                     component="indicator_calculation",
                     indicator_code=self.INDICATOR_CODE,
                     temporality=self.temporality)
            else:
                error("Indicator calculation failed",
                      component="indicator_calculation",
                      indicator_code=self.INDICATOR_CODE,
                      temporality=self.temporality)
            
            return result
            
        except Exception as e:
            error("Exception during indicator calculation",
                  component="indicator_calculation",
                  indicator_code=self.INDICATOR_CODE,
                  error=str(e))
            return False

    def _validate_required_attributes(self):
        """Validate that required class attributes are defined"""
        if not self.INDICATOR_CODE:
            raise ValueError(f"INDICATOR_CODE must be defined in {self.__class__.__name__}")
        
        if not self.SUPPORTED_TEMPORALITIES:
            raise ValueError(f"SUPPORTED_TEMPORALITIES must be defined in {self.__class__.__name__}")

    def _generate_output_filename(self, suffix: str = "") -> str:
        """
        Generate standardized output filename.
        
        Args:
            suffix: Optional suffix to add to filename
            
        Returns:
            str: Generated filename
        """
        try:
            # Use naming config template if available
            if self.naming_config and 'file_naming' in self.naming_config:
                template = self.naming_config['file_naming'].get('template', '')
                components = self.naming_config['file_naming'].get('components', {})
                
                # Generate date string based on temporality
                if self.temporality == 'annual':
                    date_str = f"{self.start_date[:4]}-{self.end_date[:4]}"
                else:
                    date_str = f"{self.start_date}-{self.end_date}"
                
                filename = template.format(
                    temporal=self.temporality,
                    country=self.country_code,
                    variable=self.short_name.lower(),
                    date=date_str
                )
                
                if suffix:
                    name, ext = filename.rsplit('.', 1) if '.' in filename else (filename, 'tif')
                    filename = f"{name}_{suffix}.{ext}"
                
                return filename
            
            # Fallback filename format
            date_str = f"{self.start_date}-{self.end_date}"
            suffix_str = f"_{suffix}" if suffix else ""
            return f"{self.temporality}_{self.country_code}_{self.short_name.lower()}_{date_str}{suffix_str}.tif"
            
        except Exception as e:
            warning("Failed to generate filename from config, using fallback",
                   component="indicator_calculation",
                   error=str(e))
            
            # Simple fallback
            return f"{self.short_name.lower()}_{self.temporality}_{self.country_code}.tif"

    # Abstract methods that must be implemented by subclasses
    @abstractmethod
    def calculate_annual(self) -> bool:
        """Calculate annual indicator values"""
        pass

    def calculate_monthly(self) -> bool:
        """Calculate monthly indicator values (optional)"""
        warning("Monthly calculation not implemented",
               component="indicator_calculation",
               indicator_code=self.INDICATOR_CODE)
        return False

    def calculate_daily(self) -> bool:
        """Calculate daily indicator values (optional)"""
        warning("Daily calculation not implemented",
               component="indicator_calculation",
               indicator_code=self.INDICATOR_CODE)
        return False

    def calculate_seasonal(self) -> bool:
        """Calculate seasonal indicator values (optional)"""
        warning("Seasonal calculation not implemented",
               component="indicator_calculation",
               indicator_code=self.INDICATOR_CODE)
        return False

    def _generate_climate_index_filename(self, year_or_suffix) -> str:
        """
        Generate filename in climate_index format: climate_index_{iso2}_{shortname}_{year}.tif
        
        Args:
            year_or_suffix: Year (int) or suffix string like "mean"
            
        Returns:
            str: Generated filename
        """
        try:
            iso2 = self.country_code.lower()
            short_name = self.short_name # Use dynamic short_name from DB
            
            if isinstance(year_or_suffix, int):
                # For specific year: climate_index_co_ndd_2024.tiff
                return f"climate_index_{self.temporality}_{iso2}_{short_name}_{year_or_suffix}.tif"
            else:
                # For multi-year averages: climate_index_co_ndd_mean.tiff
                return f"climate_index_{self.temporality}_{iso2}_{short_name}_{year_or_suffix}.tif"
                
        except Exception as e:
            error("Failed to generate climate index filename",
                  component="indicator_calculation",
                  indicator_code=getattr(self, 'INDICATOR_CODE', 'unknown'),
                  error=str(e))
            # Fallback
            fallback_short = getattr(self, 'short_name', 'unknown')
            return f"climate_index_{self.temporality}_{self.country_code.lower()}_{fallback_short}_{year_or_suffix}.tif"