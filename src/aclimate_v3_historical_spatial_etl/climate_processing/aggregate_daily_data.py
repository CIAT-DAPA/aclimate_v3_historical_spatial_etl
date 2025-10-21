import xarray as xr
import rioxarray
import numpy as np
from collections import defaultdict
from pathlib import Path
from typing import Optional, Union, List, Dict
from ..tools import error, warning, info

class MonthlyProcessor:
    def __init__(
        self,
        input_path: Union[str, Path],
        output_path: Union[str, Path],
        naming_config: Dict,
        countries_config: Dict,
        country: Optional[str] = None
    ):
        """
        Processes daily rasters into monthly averages.
        
        Args:
            input_path: Path to daily clipped rasters
            output_path: Path to save monthly averaged rasters
            naming_config: Dict with naming conventions configuration
            countries_config: Dict with country information configuration
            country: Target country name
        """
        try:
            # Convert paths to Path objects
            self.input_path = Path(input_path) if isinstance(input_path, str) else input_path
            self.output_path = Path(output_path) if isinstance(output_path, str) else output_path
            
            # Validate input path
            if not self.input_path.exists():
                raise ValueError(f"Input path does not exist: {self.input_path}")

            # Create output directory
            self.output_path.mkdir(parents=True, exist_ok=True)
            
            self._load_naming_config(naming_config)
            self._load_countries_config(countries_config, country)
            
            info("MonthlyProcessor initialized successfully",
                 component="initialization",
                 input_path=str(self.input_path),
                 output_path=str(self.output_path),
                 country=country)
                
        except Exception as e:
            error("Failed to initialize MonthlyProcessor",
                  component="initialization",
                  error=str(e))
            raise
        
    def _load_naming_config(self, config: Dict):
        """Load naming configuration from dictionary"""
        try:
            self.naming_config = config
            self.template = self.naming_config['file_naming']['template']
            self.variable_mapping = self.naming_config['file_naming']['components']['variable_mapping']
            
            info("Naming configuration loaded",
                 component="config",
                 template=self.template)
                
        except Exception as e:
            error("Failed to load naming configuration",
                  component="config",
                  error=str(e))
            raise
    
    def _load_countries_config(self, config: Dict, country: Optional[str]):
        """Load country configuration and set country code from dict"""
        try:
            target_country = country or config.get('default_country')
            if not target_country:
                raise ValueError("No country specified and no default country in config")
                
            country_config = config['countries'].get(target_country.upper())
            if not country_config:
                raise ValueError(f"Country '{target_country}' not found in configuration")
                
            self.country_code = country_config['iso2_code'].lower()
            
            info("Country configuration loaded",
                 component="config",
                 country=target_country,
                 iso2_code=self.country_code)
                
        except Exception as e:
            error("Failed to load country configuration",
                  component="config",
                  error=str(e))
            raise

    def _generate_output_name(self, variable: str, year_month: str) -> str:
        """Generate output filename according to config template"""
        try:
            standardized_var = variable.replace(" ", "_").replace("-", "_")
            var_code = self.variable_mapping.get(standardized_var, standardized_var.lower())
            
            filename = self.template.format(
                temporal="monthly",
                country=self.country_code,
                variable=var_code,
                date=year_month
            )
            
            info("Generated output filename",
                 component="processing",
                 variable=variable,
                 year_month=year_month,
                 output_name=filename)
                
            return filename
            
        except Exception as e:
            error("Failed to generate output filename",
                  component="processing",
                  variable=variable,
                  year_month=year_month,
                  error=str(e))
            raise
    
    def process_monthly_averages(self):
        """Process all variables in input_path to generate monthly averages"""
        try:
            info("Starting monthly averaging process",
                 component="processing",
                 input_path=str(self.input_path),
                 output_path=str(self.output_path))
            
            variables_processed = 0
            months_processed = 0
            total_files = 0
            
            for var_dir in self.input_path.glob('*'):
                if not var_dir.is_dir():
                    continue
                    
                variable_name = var_dir.name
                info(f"Processing variable",
                     component="processing",
                     variable=variable_name)
                
                monthly_files = defaultdict(list)
                
                for year_dir in var_dir.glob('*'):
                    if not year_dir.is_dir():
                        continue
                        
                    for daily_file in year_dir.glob('*.tif'):
                        try:
                            date_part = daily_file.stem.split('_')[-1]
                            year_month = date_part[:6]
                            monthly_files[year_month].append(daily_file)
                            total_files += 1
                        except (IndexError, AttributeError) as e:
                            warning("Could not parse date from filename",
                                    component="processing",
                                    file=daily_file.name,
                                    error=str(e))
                            continue
                
                for year_month, files in monthly_files.items():
                    self._process_month(variable_name, year_month, files)
                    months_processed += 1
                
                variables_processed += 1
            
            info("Monthly averaging completed",
                 component="processing",
                 variables_processed=variables_processed,
                 months_processed=months_processed,
                 total_files_processed=total_files)
                
        except Exception as e:
            error("Monthly averaging process failed",
                  component="processing",
                  error=str(e))
            raise
    
    def _process_month(self, variable: str, year_month: str, files: List[Path]):
        """Process a single month's data for one variable"""
        try:
            output_filename = self._generate_output_name(variable, year_month)
            output_var_dir = self.output_path / variable
            output_var_dir.mkdir(parents=True, exist_ok=True)
            
            output_file = output_var_dir / output_filename
            
            if output_file.exists():
                info("Skipping existing output file",
                     component="processing",
                     file=str(output_file))
                return
                
            info(f"Processing month",
                 component="processing",
                 variable=variable,
                 year_month=year_month,
                 days_available=len(files))
            
            datasets = []
            for f in files:
                try:
                    ds = rioxarray.open_rasterio(f)
                    datasets.append(ds)
                except Exception as e:
                    warning("Failed to open raster file",
                            component="processing",
                            file=str(f),
                            error=str(e))
                    continue
            
            if not datasets:
                warning("No valid raster files for month",
                        component="processing",
                        variable=variable,
                        year_month=year_month)
                return
            
            combined = xr.concat(datasets, dim='time')
            
            if variable.lower() in ['prec', 'precipitation', 'et0', 'evapotranspiration']:
                monthly_data = combined.sum(dim='time', skipna=True)
                operation = "sum"
            else:
                monthly_data = combined.mean(dim='time', skipna=True)
                operation = "mean"
            
            monthly_data.rio.to_raster(output_file)
            
            info("Monthly processing completed",
                 component="processing",
                 variable=variable,
                 year_month=year_month,
                 operation=operation,
                 output_file=str(output_file))
                
        except Exception as e:
            error("Failed to process month",
                  component="processing",
                  variable=variable,
                  year_month=year_month,
                  error=str(e))
            raise
    
    def clean_processed_data(self, confirm: bool = False) -> int:
        """
        Deletes all processed monthly raster files while maintaining directory structure.
        
        Args:
            confirm: If True, asks for confirmation before deletion
            
        Returns:
            Number of files deleted
        """
        try:
            if not self.output_path.exists():
                warning("Processed data directory does not exist",
                        component="cleanup",
                        path=str(self.output_path))
                return 0
                
            tif_files = list(self.output_path.glob('**/*.tif'))
            total_files = len(tif_files)
            
            if total_files == 0:
                info("No processed raster files found to delete",
                     component="cleanup")
                return 0
                
            if confirm:
                response = input(
                    f"Are you sure you want to delete {total_files} processed raster files in {self.output_path}? [y/N]: "
                )
                if response.lower() != 'y':
                    info("Deletion cancelled by user",
                         component="cleanup")
                    return 0
                    
            deleted_count = 0
            for tif_file in tif_files:
                try:
                    tif_file.unlink()
                    deleted_count += 1
                except Exception as e:
                    warning(f"Failed to delete file {str(tif_file)}",
                            component="cleanup",
                            file=str(tif_file),
                            error=str(e))
            
            info("Cleanup completed",
                 component="cleanup",
                 files_deleted=deleted_count,
                 total_files=total_files)
            
            return deleted_count
            
        except Exception as e:
            error("Cleanup process failed",
                  component="cleanup",
                  error=str(e))
            raise