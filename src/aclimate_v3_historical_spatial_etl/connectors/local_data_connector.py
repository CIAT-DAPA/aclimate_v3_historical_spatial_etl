import os
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Tuple
from ..tools import error, warning, info

class LocalDataConnector:
    """
    Connector for managing local data repository integration.
    Handles validation of existing data and storage of newly downloaded files.
    """
    
    def __init__(self, config: Dict, local_data_path: str, copernicus_config: Dict = None, chirps_config: Dict = None):
        """
        Initialize local data connector.
        
        Args:
            config: Dictionary with local data configuration
            local_data_path: Path to local data repository
            copernicus_config: Copernicus downloader configuration
            chirps_config: CHIRPS downloader configuration
        """
        self.config = config
        self.local_data_path = Path(local_data_path)
        self.copernicus_config = copernicus_config
        self.chirps_config = chirps_config
        
        if not self.config.get('enabled', False):
            warning("Local data connector is disabled in configuration",
                   component="local_data")
            return
            
        info("Local data connector initialized",
             component="local_data",
             local_data_path=str(self.local_data_path))
        
        self._validate_paths()
    
    def _validate_paths(self):
        """Validate that local data path exists and is accessible"""
        try:
            if not self.local_data_path.exists():
                warning(f"Local data path does not exist: {self.local_data_path}",
                       component="local_data")
                info("Local data path will be created when needed",
                    component="local_data")
        except Exception as e:
            warning(f"Could not validate local data path: {str(e)}",
                   component="local_data")
    
    def check_local_availability(self, variable: str, start_date: str, end_date: str) -> Dict[str, List[str]]:
        """
        Check which dates are available locally for a given variable.
        
        Args:
            variable: Variable name (tmax, tmin, prec, rad)
            start_date: Start date (YYYY-MM or YYYY-MM-DD)
            end_date: End date (YYYY-MM or YYYY-MM-DD)
            
        Returns:
            Dict with 'available_locally' and 'missing_locally' date lists
        """
        if not self.config.get('enabled', False):
            return {'available_locally': [], 'missing_locally': self._generate_date_list(start_date, end_date)}
        
        info(f"Checking local availability for {variable}",
             component="local_data",
             variable=variable,
             date_range=f"{start_date} to {end_date}")
        
        date_list = self._generate_date_list(start_date, end_date)
        available = []
        missing = []
        
        for date_str in date_list:
            if self._check_file_exists(variable, date_str):
                available.append(date_str)
            else:
                missing.append(date_str)
        
        info(f"Local availability check completed for {variable}",
             component="local_data",
             available_count=len(available),
             missing_count=len(missing))
        
        return {
            'available_locally': available,
            'missing_locally': missing
        }
    
    def _generate_date_list(self, start_date: str, end_date: str) -> List[str]:
        """Generate list of dates between start and end date"""
        try:
            # Handle both YYYY-MM and YYYY-MM-DD formats
            if len(start_date.split('-')) == 2:
                # Monthly format - generate daily dates for the month(s)
                start = datetime.strptime(start_date, "%Y-%m")
                end = datetime.strptime(end_date, "%Y-%m")
                dates = []
                
                current = start
                while current <= end:
                    # Get last day of current month
                    if current.month == 12:
                        next_month = current.replace(year=current.year + 1, month=1)
                    else:
                        next_month = current.replace(month=current.month + 1)
                    
                    last_day = (next_month - timedelta(days=1)).day
                    
                    # Generate all days in current month
                    for day in range(1, last_day + 1):
                        date_obj = current.replace(day=day)
                        dates.append(date_obj.strftime("%Y-%m-%d"))
                    
                    # Move to next month
                    if current.month == 12:
                        current = current.replace(year=current.year + 1, month=1)
                    else:
                        current = current.replace(month=current.month + 1)
                
                return dates
            else:
                # Daily format
                start = datetime.strptime(start_date, "%Y-%m-%d")
                end = datetime.strptime(end_date, "%Y-%m-%d")
                dates = []
                
                current = start
                while current <= end:
                    dates.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=1)
                
                return dates
                
        except ValueError as e:
            error(f"Error generating date list: {str(e)}",
                  component="local_data")
            return []
    
    def _check_file_exists(self, variable: str, date_str: str) -> bool:
        """Check if file exists for given variable and date"""
        try:
            file_path = self._get_local_file_path(variable, date_str)
            exists = file_path.exists()
            
            if exists:
                info(f"Found local file: {file_path.name}",
                     component="local_data",
                     variable=variable,
                     date=date_str)
            
            return exists
        except Exception as e:
            warning(f"Error checking file existence for {variable} {date_str}: {str(e)}",
                   component="local_data")
            return False
    
    def _get_local_file_path(self, variable: str, date_str: str) -> Path:
        """Construct local file path for variable and date"""
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        year = date_obj.strftime("%Y")
        
        # First check if variable exists in Copernicus configuration
        if (self.copernicus_config and 
            'datasets' in self.copernicus_config and
            self.copernicus_config.get('default_dataset')):
            
            default_dataset = self.copernicus_config['default_dataset']
            dataset_config = self.copernicus_config['datasets'].get(default_dataset, {})
            copernicus_variables = dataset_config.get('variables', {})
            
            if variable in copernicus_variables:
                # Copernicus data
                source_config = self.config['sources']['copernicus']
                var_config = source_config['variables'][variable]
                
                # Format date for filename (yyyymmdd)
                formatted_date = date_obj.strftime("%Y%m%d")
                file_pattern = var_config['file_pattern'].replace('{date}', formatted_date)
                
                file_path = (self.local_data_path / 
                            source_config['base_folder'] / 
                            var_config['folder_name'] / 
                            year / 
                            file_pattern)
                return file_path
        
        # Then check if variable exists in CHIRPS configuration
        if (self.chirps_config and 
            'datasets' in self.chirps_config):
            
            chirps_variables = self.chirps_config.get('datasets', {})
            
            if variable in chirps_variables:
                # CHIRPS data
                source_config = self.config['sources']['chirps']
                
                # The variable must exist in the local config sources
                if variable not in source_config['variables']:
                    raise ValueError(f"Variable '{variable}' found in CHIRPS config but not in local_data_config sources")
                
                var_config = source_config['variables'][variable]
                
                # Format date for filename (yyyy.mm.dd)
                formatted_date = date_obj.strftime("%Y.%m.%d")
                file_pattern = var_config['file_pattern'].replace('{date}', formatted_date)
                
                # CHIRPS path structure (note: folder_name might be empty)
                if var_config['folder_name']:
                    file_path = (self.local_data_path / 
                                source_config['base_folder'] / 
                                var_config['folder_name'] /
                                year / 
                                file_pattern)
                else:
                    file_path = (self.local_data_path / 
                                source_config['base_folder'] / 
                                year / 
                                file_pattern)
                return file_path
        
        # If variable is not found in any configuration
        raise ValueError(f"Variable '{variable}' not found in Copernicus or CHIRPS configurations")
    
    def copy_local_file(self, variable: str, date_str: str, destination_path: str) -> bool:
        """
        Copy file from local repository to destination with proper directory structure.
        
        Args:
            variable: Variable name
            date_str: Date string (YYYY-MM-DD)
            destination_path: Base destination path (raw_data directory)
            
        Returns:
            True if successful, False otherwise
        """
        if not self.config.get('enabled', False):
            return False
            
        try:
            source_path = self._get_local_file_path(variable, date_str)
            base_dest_path = Path(destination_path)
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            year = date_obj.strftime("%Y")
            
            if not source_path.exists():
                warning(f"Source file does not exist: {source_path}",
                       component="local_data")
                return False
            
            # Determine the proper destination structure based on variable type
            if (self.copernicus_config and 
                'datasets' in self.copernicus_config and
                self.copernicus_config.get('default_dataset')):
                
                default_dataset = self.copernicus_config['default_dataset']
                dataset_config = self.copernicus_config['datasets'].get(default_dataset, {})
                copernicus_variables = dataset_config.get('variables', {})
                
                if variable in copernicus_variables:
                    # Copernicus structure: raw_data/{output_dir}/{year}/
                    var_config = copernicus_variables[variable]
                    output_dir = var_config.get('output_dir', variable)
                    dest_dir = base_dest_path / output_dir / year
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Keep original filename for .nc files (they need processing)
                    dest_file = dest_dir / source_path.name
                    
                    # Copy file
                    shutil.copy2(source_path, dest_file)
                    
                    info(f"Successfully copied Copernicus local file",
                         component="local_data",
                         variable=variable,
                         date=date_str,
                         source=str(source_path),
                         destination=str(dest_file))
                    
                    return True
            
            # Check CHIRPS
            if (self.chirps_config and 
                'datasets' in self.chirps_config):
                
                chirps_variables = self.chirps_config.get('datasets', {})
                
                if variable in chirps_variables:
                    # CHIRPS structure: raw_data/{output_dir}/{year}/
                    chirps_config = chirps_variables[variable]
                    output_dir = chirps_config.get('output_dir', variable)
                    dest_dir = base_dest_path / output_dir / year
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    
                    # CHIRPS files need to be renamed to match expected pattern
                    # From: chirps-v3.0.2025.04.05.tif
                    # To: Precipitation_20250405.tif (or similar based on config)
                    file_naming = chirps_config.get('file_naming', f"{output_dir.title()}_{date_str.replace('-', '')}.tif")
                    if '{variable}' in file_naming and '{date}' in file_naming:
                        dest_filename = file_naming.format(
                            variable=output_dir.title(),
                            date=date_str.replace('-', '')
                        )
                    else:
                        dest_filename = f"{output_dir.title()}_{date_str.replace('-', '')}.tif"
                    
                    dest_file = dest_dir / dest_filename
                    
                    # Copy file
                    shutil.copy2(source_path, dest_file)
                    
                    info(f"Successfully copied CHIRPS local file",
                         component="local_data",
                         variable=variable,
                         date=date_str,
                         source=str(source_path),
                         destination=str(dest_file))
                    
                    return True
            
            # If we get here, variable wasn't found in either config
            error(f"Variable {variable} not found in downloader configurations",
                  component="local_data",
                  variable=variable)
            return False
            
        except Exception as e:
            error(f"Error copying local file for {variable} {date_str}: {str(e)}",
                  component="local_data",
                  error_details=str(e))
            return False
    
    def save_downloaded_file(self, source_file: str, variable: str, date_str: str) -> bool:
        """
        Save downloaded file to local repository.
        
        Args:
            source_file: Path to downloaded file
            variable: Variable name
            date_str: Date string (YYYY-MM-DD)
            
        Returns:
            True if successful, False otherwise
        """
        if not self.config.get('enabled', False):
            info("Local data connector is disabled - skipping save",
                 component="local_data")
            return False
            
        try:
            source_path = Path(source_file)
            if not source_path.exists():
                warning(f"Source file does not exist for saving: {source_path}",
                       component="local_data")
                return False
            
            info(f"Attempting to save downloaded file to local repository",
                 component="local_data",
                 variable=variable,
                 date=date_str,
                 source=str(source_path))
            
            dest_path = self._get_local_file_path(variable, date_str)
            
            info(f"Determined destination path for local save",
                 component="local_data",
                 destination=str(dest_path))
            
            # Create destination directory if it doesn't exist
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Copy file to local repository
            shutil.copy2(source_path, dest_path)
            
            info(f"Successfully saved file to local repository",
                 component="local_data",
                 variable=variable,
                 date=date_str,
                 destination=str(dest_path))
            
            return True
            
        except Exception as e:
            error(f"Error saving file to local repository for {variable} {date_str}: {str(e)}",
                  component="local_data",
                  error_details=str(e))
            return False
    
    def get_available_variables(self, start_date: str, end_date: str) -> Dict[str, Dict[str, List[str]]]:
        """
        Get availability status for all variables.
        
        Returns:
            Dict with variable availability information
        """
        if not self.config.get('enabled', False):
            return {}
        
        # Extract variables dynamically from configurations
        variables = []
        
        # Get Copernicus variables if config is available
        if self.copernicus_config:
            default_dataset = self.copernicus_config.get('default_dataset')
            if default_dataset and 'datasets' in self.copernicus_config:
                dataset_config = self.copernicus_config['datasets'].get(default_dataset, {})
                copernicus_vars = list(dataset_config.get('variables', {}).keys())
                variables.extend(copernicus_vars)
        
        # Get CHIRPS variables if config is available
        if self.chirps_config:
            chirps_vars = list(self.chirps_config.get('datasets', {}).keys())
            variables.extend(chirps_vars)
        
        if not variables:
            warning("No variables found in configurations",
                   component="local_data")
            return {}
        
        info(f"Checking availability for variables: {variables}",
             component="local_data")
        
        availability = {}
        for variable in variables:
            availability[variable] = self.check_local_availability(variable, start_date, end_date)
        
        return availability