"""
Validation utilities for ETL pipeline arguments and data.
"""
from datetime import datetime
from typing import Tuple
from .logging_manager import error, info


class ETLError(Exception):
    """Custom exception for ETL pipeline errors"""
    pass


def validate_dates(start_date: str, end_date: str):
    """Validate date format and range."""
    try:
        info("Validating date range", 
             component="validation",
             start_date=start_date,
             end_date=end_date)
        
        start = datetime.strptime(start_date, "%Y-%m")
        end = datetime.strptime(end_date, "%Y-%m")
        if start > end:
            raise ETLError("Start date must be before end date")
            
        info("Date validation successful", component="validation")
    except ValueError as e:
        error("Invalid date format", 
              component="validation",
              error=str(e))
        raise ETLError(f"Invalid date format. Use YYYY-MM. Error: {str(e)}")


def validate_indicator_years(indicator_years: str) -> Tuple[str, str]:
    """
    Validate and parse indicator year range.
    
    Args:
        indicator_years: Year range string in format 'YYYY-YYYY' or single year 'YYYY'
        
    Returns:
        Tuple of (start_year, end_year)
    """
    try:
        if not indicator_years:
            raise ValueError("Indicator years range is required")
        
        # Handle single year format
        if '-' not in indicator_years:
            try:
                year = int(indicator_years)
                if year < 1900 or year > 2030:
                    raise ValueError("Year must be between 1900 and 2030")
                
                info("Single year indicator calculation",
                     component="validation",
                     year=year)
                
                return str(year), str(year)
            except ValueError as e:
                if "must be between" in str(e):
                    raise e
                raise ValueError("Invalid year format. Use 'YYYY' or 'YYYY-YYYY' format")
        
        # Handle year range format
        start_year_str, end_year_str = indicator_years.split('-', 1)
        
        # Validate year format
        start_year = int(start_year_str)
        end_year = int(end_year_str)
        
        if start_year > end_year:
            raise ValueError("Start year must be before or equal to end year")
        
        if start_year < 1900 or end_year > 2030:
            raise ValueError("Years must be between 1900 and 2030")
        
        info("Indicator years validation successful",
             component="validation",
             start_year=start_year,
             end_year=end_year)
        
        return str(start_year), str(end_year)
        
    except ValueError as e:
        error("Invalid indicator years format",
              component="validation",
              indicator_years=indicator_years,
              error=str(e))
        raise ETLError(f"Invalid indicator years format. Use 'YYYY' or 'YYYY-YYYY'. Error: {str(e)}")