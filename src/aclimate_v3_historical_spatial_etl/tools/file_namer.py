from datetime import datetime
from pathlib import Path
import json

class FileNamer:
    def __init__(self, naming_config_path: str, clipping_config_path: str):
        with open(naming_config_path) as f:
            self.naming_config = json.load(f)
        with open(clipping_config_path) as f:
            self.clipping_config = json.load(f)
    
    def get_output_filename(self, variable: str, date: str, country: str) -> str:
        """Generate standardized output filename"""
        template = self.naming_config['file_naming']['template']
        components = self.naming_config['file_naming']['components']
        
        # Get variable code
        var_mapping = components['variable_mapping']
        var_code = var_mapping.get(variable, variable.lower())
        
        # Get country code
        country_code = self.clipping_config['countries'][country.upper()]['iso2_code']
        
        # Format date (assuming input format is YYYYMMDD)
        date_obj = datetime.strptime(date, "%Y%m%d")
        formatted_date = date_obj.strftime("%Y%m%d")
        
        return template.format(
            temporal=components['temporal'],
            country=country_code,
            variable=var_code,
            date=formatted_date
        )