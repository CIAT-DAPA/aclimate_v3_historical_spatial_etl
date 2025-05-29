import json
from pathlib import Path
import xarray as xr
import rioxarray
import numpy as np
from collections import defaultdict
from typing import Optional, Union

class MonthlyProcessor:
    def __init__(
        self,
        input_path: Union[str, Path],
        output_path: Union[str, Path],
        naming_config_path: Union[str, Path],
        countries_config_path: Union[str, Path],
        country: Optional[str] = None
    ):
        """
        Processes daily rasters into monthly averages.
        
        Args:
            input_path: Path to daily clipped rasters (as string or Path object)
            output_path: Path to save monthly averaged rasters (as string or Path object)
            naming_config_path: JSON config path (as string or Path object)
            countries_config_path: JSON config path (as string or Path object)
            country: Target country name
        """
        # Convert all paths to Path objects if they're strings
        self.input_path = Path(input_path) if isinstance(input_path, str) else input_path
        self.output_path = Path(output_path) if isinstance(output_path, str) else output_path
        naming_config_path = Path(naming_config_path) if isinstance(naming_config_path, str) else naming_config_path
        countries_config_path = Path(countries_config_path) if isinstance(countries_config_path, str) else countries_config_path
        self.output_path = self.output_path / "monthly_data"
        # Validate paths
        if not self.input_path.exists():
            raise ValueError(f"Input path does not exist: {self.input_path}")
        if not naming_config_path.exists():
            raise ValueError(f"Naming config file not found: {naming_config_path}")
        if not countries_config_path.exists():
            raise ValueError(f"Countries config file not found: {countries_config_path}")

        # Create output directory if it doesn't exist
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        # Load configurations
        self._load_naming_config(naming_config_path)
        self._load_countries_config(countries_config_path, country)
        
    def _load_naming_config(self, config_path: Path):
        """Load naming configuration from JSON file"""
        with open(config_path) as f:
            self.naming_config = json.load(f)
        
        # Get components
        self.template = self.naming_config['file_naming']['template']
        self.variable_mapping = self.naming_config['file_naming']['components']['variable_mapping']
        
    def _load_countries_config(self, config_path: Path, country: Optional[str]):
        """Load country configuration and set country code"""
        with open(config_path) as f:
            countries_config = json.load(f)
        
        # Determine country to use
        target_country = country or countries_config.get('default_country')
        if not target_country:
            raise ValueError("No country specified and no default country in config")
            
        # Get country configuration
        country_config = countries_config['countries'].get(target_country.upper())
        if not country_config:
            raise ValueError(f"Country '{target_country}' not found in configuration")
            
        self.country_code = country_config['iso2_code'].lower()
        
    def _generate_output_name(self, variable: str, year_month: str) -> str:
        """Generate output filename according to config template"""
        # Standardize variable name (handle spaces, hyphens, etc.)
        standardized_var = variable.replace(" ", "_").replace("-", "_")
        
        # Get standardized variable code from mapping
        var_code = self.variable_mapping.get(
            standardized_var,
            standardized_var.lower()
        )
        
        return self.template.format(
            temporal="monthly",  # Changed from daily
            country=self.country_code,
            variable=var_code,
            date=year_month
        )

    def process_monthly_averages(self):
        """Process all variables in input_path to generate monthly averages"""
        # Process each variable directory
        for var_dir in self.input_path.glob('*'):
            if not var_dir.is_dir():
                continue
                
            variable_name = var_dir.name
            print(f"\nProcessing variable: {variable_name}")
            
            # Organize files by year-month
            monthly_files = defaultdict(list)
            
            # Find all yearly directories
            for year_dir in var_dir.glob('*'):
                if not year_dir.is_dir():
                    continue
                    
                # Find all daily files for this year
                for daily_file in year_dir.glob('*.tif'):
                    # Extract date from filename (format: ..._YYYYMMDD.tif)
                    try:
                        date_part = daily_file.stem.split('_')[-1]
                        year_month = date_part[:6]  # Get YYYYMM
                        monthly_files[year_month].append(daily_file)
                    except (IndexError, AttributeError):
                        print(f"Warning: Could not parse date from {daily_file.name}")
                        continue
            
            # Process each month
            for year_month, files in monthly_files.items():
                self._process_month(variable_name, year_month, files)
    
    def _process_month(self, variable: str, year_month: str, files: list):
        """Process a single month's data for one variable"""
        # Generate output filename from config template
        output_filename = self._generate_output_name(variable, year_month)
        
        # Create output directory structure (output_path/Variable)
        output_var_dir = self.output_path / variable
        output_var_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_var_dir / output_filename
        
        if output_file.exists():
            print(f"Skipping existing file: {output_file}")
            return
            
        print(f"Processing {variable} for {year_month} ({len(files)} days)")
        
        try:
            # Open and stack all daily rasters
            datasets = []
            for f in files:
                ds = rioxarray.open_rasterio(f)
                datasets.append(ds)
            
            # Combine and compute monthly mean
            combined = xr.concat(datasets, dim='time')
            monthly_mean = combined.mean(dim='time', skipna=True)
            
            # Write output
            monthly_mean.rio.to_raster(output_file)
            print(f"Saved monthly average: {output_file}")
            
        except Exception as e:
            print(f"Error processing {variable} {year_month}: {str(e)}")


    def clean_processed_data(self, confirm: bool = False) -> int:
        """
        Deletes all processed monthly raster files while maintaining directory structure.
        
        Args:
            confirm: If True, asks for confirmation before deletion (safety measure)
            
        Returns:
            Number of files deleted
        """
        if not self.output_path.exists():
            print(f"Processed data directory does not exist: {self.output_path}")
            return 0
            
        # Count all .tif files in output directory
        tif_files = list(self.output_path.glob('**/*.tif'))
        total_files = len(tif_files)
        
        if total_files == 0:
            print("No processed raster files found to delete")
            return 0
            
        if confirm:
            response = input(
                f"Are you sure you want to delete {total_files} processed raster files in {self.output_path}? [y/N]: "
            )
            if response.lower() != 'y':
                print("Deletion cancelled")
                return 0
                
        deleted_count = 0
        for tif_file in tif_files:
            try:
                tif_file.unlink()
                deleted_count += 1
            except Exception as e:
                print(f"Error deleting {tif_file}: {str(e)}")
                
        print(f"Successfully deleted {deleted_count}/{total_files} files")
        return deleted_count