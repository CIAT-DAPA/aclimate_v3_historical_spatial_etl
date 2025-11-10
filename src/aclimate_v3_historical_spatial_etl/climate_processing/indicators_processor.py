import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from ..tools import info, error, warning, exception
from .indicators import CalculatorLoader
from aclimate_v3_orm.services import (
    MngCountryIndicatorService, 
    MngIndicatorService, 
    MngCountryService
)



class IndicatorsProcessor:
    """
    Main processor for climate indicators calculation.
    
    This class coordinates the calculation of all indicators configured for a specific country.
    It queries the database to get the country's indicators and orchestrates their calculation.
    """
    
    def __init__(
        self,
        country: str,
        start_date: str,
        end_date: str,
        output_path: Union[str, Path],
        naming_config: Dict,
        countries_config: Dict
    ):
        """
        Initialize the indicators processor.
        
        Args:
            country: Target country name (e.g., "COLOMBIA", "HONDURAS")
            start_date: Start date in YYYY-MM format
            end_date: End date in YYYY-MM format
            output_path: Path to save calculated indicators
            naming_config: Dictionary with naming conventions configuration
            countries_config: Dictionary with country information configuration
        """
        try:
            self.country = country.upper()
            self.start_date = start_date
            self.end_date = end_date
            
            # Convert paths to Path objects
            self.output_path = Path(output_path) if isinstance(output_path, str) else output_path
            self.output_path.mkdir(parents=True, exist_ok=True)
            
            # Load configurations
            self._load_naming_config(naming_config)
            self._load_countries_config(countries_config)
            
            # Initialize ORM services
            self.country_indicator_service = MngCountryIndicatorService()
            self.indicator_service = MngIndicatorService()
            self.country_service = MngCountryService()
            
            # Cache for country and indicators data
            self.country_data = None
            self.country_indicators = []
            self.indicators_registry = {}
            
            info("IndicatorsProcessor initialized successfully",
                 component="indicators_init",
                 country=self.country,
                 start_date=start_date,
                 end_date=end_date,
                 output_path=str(self.output_path))
                
        except Exception as e:
            error("Failed to initialize IndicatorsProcessor",
                  component="indicators_init",
                  country=country,
                  error=str(e))
            raise

    def _load_naming_config(self, config: Dict):
        """Load naming configuration from dict"""
        try:
            if not config:
                raise ValueError("Naming configuration is required")
            
            self.naming_config = config
            
            info("Naming configuration loaded successfully",
                 component="indicators_config")
                
        except Exception as e:
            error("Failed to load naming configuration",
                  component="indicators_config",
                  error=str(e))
            raise

    def _load_countries_config(self, config: Dict):
        """Load country configuration and set country code from dict"""
        try:
            if not config:
                raise ValueError("Countries configuration is required")
                
            country_config = config['countries'].get(self.country)
            if not country_config:
                raise ValueError(f"Country '{self.country}' not found in configuration")
                
            self.country_code = country_config['iso2_code'].lower()
            self.countries_config = config

            info("Country configuration loaded successfully",
                 component="indicators_config",
                 country=self.country,
                 iso2_code=self.country_code)
                
        except Exception as e:
            error("Failed to load country configuration",
                  component="indicators_config",
                  country=self.country,
                  error=str(e))
            raise

    def _get_country_data(self) -> Dict[str, Any]:
        """Get country data from database"""
        try:
            if self.country_data is None:
                info("Fetching country data from database",
                     component="indicators_db",
                     country=self.country)
                
                countries = self.country_service.get_by_name(self.country)
                if not countries:
                    raise ValueError(f"Country '{self.country}' not found in database")
                
                # Take the first match (should be unique)
                self.country_data = countries[0].model_dump()
                
                info("Country data retrieved successfully",
                     component="indicators_db",
                     country=self.country,
                     country_id=self.country_data['id'])
            
            return self.country_data
            
        except Exception as e:
            error("Failed to retrieve country data",
                  component="indicators_db",
                  country=self.country,
                  error=str(e))
            raise

    def _get_country_indicators(self) -> List[Dict[str, Any]]:
        """Get all indicators configured for the country"""
        try:
            if not self.country_indicators:
                country_data = self._get_country_data()
                country_id = country_data['id']
                
                info("Fetching country indicators from database",
                     component="indicators_db",
                     country=self.country,
                     country_id=country_id)
                
                # Get country-indicator relationships
                country_indicators = self.country_indicator_service.get_by_country(country_id)
                
                # Filter to only include indicators with spatial_climate enabled
                country_indicators = list(filter(lambda ci: getattr(ci, 'spatial_climate', False), country_indicators))
                
                if not country_indicators:
                    warning("No indicators found for country",
                           component="indicators_db",
                           country=self.country)
                    return []
                
                # Get full indicator details for each country-indicator relationship
                indicators_data = []
                for country_indicator in country_indicators:
                    ci_dict = country_indicator.model_dump()
                    
                    # Get the full indicator details
                    indicator = self.indicator_service.get_by_id(ci_dict['indicator_id'])
                    if indicator:
                        indicator_dict = indicator.model_dump()
                        # Merge country-indicator config with indicator details
                        indicator_dict['country_config'] = ci_dict
                        indicators_data.append(indicator_dict)
                
                self.country_indicators = indicators_data
                
                info("Country indicators retrieved successfully",
                     component="indicators_db",
                     country=self.country,
                     indicators_count=len(self.country_indicators),
                     indicators=[ind['name'] for ind in self.country_indicators])
            
            return self.country_indicators
            
        except Exception as e:
            error("Failed to retrieve country indicators",
                  component="indicators_db",
                  country=self.country,
                  error=str(e))
            raise

    def _validate_date_range(self):
        """Validate date format and range"""
        try:
            info("Validating date range for indicators processing",
                 component="indicators_validation",
                 start_date=self.start_date,
                 end_date=self.end_date)
            
            start = datetime.strptime(self.start_date, "%Y-%m")
            end = datetime.strptime(self.end_date, "%Y-%m")
            
            if start > end:
                raise ValueError("Start date must be before end date")
            
            info("Date range validation successful",
                 component="indicators_validation")
                
        except ValueError as e:
            error("Invalid date format or range",
                  component="indicators_validation",
                  error=str(e))
            raise

    def process_all_indicators(self):
        """
        Main method to process all indicators for the country.
        
        This method:
        1. Validates the date range
        2. Gets all indicators configured for the country
        3. For each indicator, determines the calculation method and executes it
        4. Saves results to the output path
        """
        try:
            info("Starting indicators processing for country",
                 component="indicators_processing",
                 country=self.country,
                 start_date=self.start_date,
                 end_date=self.end_date)
            
            # Validate inputs
            self._validate_date_range()
            
            # Get country indicators
            indicators = self._get_country_indicators()
            
            if not indicators:
                warning("No indicators to process",
                       component="indicators_processing",
                       country=self.country)
                return
            
            # Process each indicator
            processed_count = 0
            failed_count = 0
            
            for indicator in indicators:
                try:
                    info("Processing indicator",
                         component="indicators_processing",
                         indicator_name=indicator['name'],
                         indicator_type=indicator.get('type', 'unknown'))
                    
                    # TODO: Here we will add the logic to route to specific indicator calculators
                    # For now, we just log the indicator details
                    self._process_single_indicator(indicator)
                    processed_count += 1
                    
                except Exception as e:
                    error("Failed to process indicator",
                          component="indicators_processing",
                          indicator_name=indicator['name'],
                          error=str(e))
                    failed_count += 1
                    continue
            
            info("Indicators processing completed",
                 component="indicators_processing",
                 country=self.country,
                 total_indicators=len(indicators),
                 processed=processed_count,
                 failed=failed_count)
                
        except Exception as e:
            error("Failed to process indicators",
                  component="indicators_processing",
                  country=self.country,
                  error=str(e))
            raise

    def _process_single_indicator(self, indicator: Dict[str, Any]):
        """
        Process a single indicator.
        
        This method will route to specific indicator calculation methods
        based on the indicator type or name.
        
        Args:
            indicator: Dictionary containing indicator details and configuration
        """
        try:
            indicator_name = indicator['name']
            indicator_type = indicator.get('type', 'unknown')
            indicator_config = indicator.get('country_config', {})
            
            info(f"Processing single indicator {indicator_name}",
                 component="indicators_calculation",
                 indicator_name=indicator_name,
                 indicator_type=indicator_type,
                 has_config=bool(indicator_config))
            
            # TODO: Implement routing logic based on indicator type/name
            # Now using auto-discovery pattern with CalculatorLoader
            
            # Create output directory for this indicator
            indicator_output_dir = self.output_path / f"{indicator_name.lower().replace(' ', '_')}"
            indicator_output_dir.mkdir(parents=True, exist_ok=True)
            
            
            # Get calculator class for this indicator
            calculator_class = CalculatorLoader.get_calculator(indicator.get('short_name', ''))
            
            if not calculator_class:
                warning(f"No calculator found for indicator '{indicator.get('short_name', '')}' - Calculator not implemented or not discovered",
                       component="indicators_calculation",
                       indicator_name=indicator_name,
                       short_name=indicator.get('short_name', ''))
                return
            
            # Create calculator instance
            calculator = calculator_class(
                indicator_config=indicator,
                output_path=indicator_output_dir,
                start_date=self.start_date,
                end_date=self.end_date,
                country_code=self.country_code,
                naming_config=self.naming_config
            )
            
            # Execute calculation
            success = calculator.calculate()
            
            if success:
                info(f"Indicator calculation completed successfully for {indicator_name}",
                     component="indicators_calculation",
                     indicator_name=indicator_name,
                     indicator_code=calculator.INDICATOR_CODE)
            else:
                error(f"Indicator calculation failed for {indicator_name}",
                      component="indicators_calculation",
                      indicator_name=indicator_name,
                      indicator_code=getattr(calculator, 'INDICATOR_CODE', 'unknown'))
            
        except Exception as e:
            error(f"Failed to process single indicator {indicator_name} error {str(e)}",
                  component="indicators_calculation",
                  indicator_name=indicator.get('name', 'unknown'),
                  error=str(e))
            raise

    def get_available_indicators(self) -> List[Dict[str, Any]]:
        """
        Get list of all indicators available for the country.
        
        Returns:
            List of dictionaries containing indicator information
        """
        try:
            return self._get_country_indicators()
        except Exception as e:
            error("Failed to get available indicators",
                  component="indicators_query",
                  country=self.country,
                  error=str(e))
            return []

    def get_indicator_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get specific indicator by name.
        
        Args:
            name: Indicator name to search for
            
        Returns:
            Dictionary containing indicator information, or None if not found
        """
        try:
            indicators = self._get_country_indicators()
            for indicator in indicators:
                if indicator['name'].lower() == name.lower():
                    return indicator
            return None
        except Exception as e:
            error("Failed to get indicator by name",
                  component="indicators_query",
                  country=self.country,
                  indicator_name=name,
                  error=str(e))
            return None