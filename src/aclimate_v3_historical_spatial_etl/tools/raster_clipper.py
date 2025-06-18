import json
from pathlib import Path
from typing import Dict, Any
from aclimate_v3_cut_spatial_data import get_clipper, GeoServerBasicAuth
import re
from .logging_manager import info, warning, error

class RasterClipper:
    def __init__(self, 
                 country: str,
                 downloader_configs: Dict[str, Path],
                 naming_config_path: Path,
                 clipping_config_path: Path):
        """
        Clips raster files to country boundaries using GeoServer.
        
        Args:
            country: Target country (e.g., "COLOMBIA")
            downloader_configs: Dictionary with {'downloader_name': Path(config_file)}
            naming_config_path: Path to the naming config file
            clipping_config_path: Path to the clipping config file
        """
        try:
            self.country = country.upper()
            self.downloader_configs = downloader_configs
            
            # Load configurations
            info("Loading configuration files",
                 component="initialization",
                 naming_config=str(naming_config_path),
                 clipping_config=str(clipping_config_path))
            
            self.naming_config = self._load_config(naming_config_path)
            self.clipping_config = self._load_config(clipping_config_path)
            
            # Validate country
            if self.country not in self.clipping_config['countries']:
                error("Country not found in configuration",
                      component="validation",
                      country=country,
                      available_countries=list(self.clipping_config['countries'].keys()))
                raise ValueError(f"Country '{country}' not found in configuration")
            
            self.country_config = self.clipping_config['countries'][self.country]
            self.conn = GeoServerBasicAuth()
            
            info("RasterClipper initialized successfully",
                 component="initialization",
                 country=country)
                
        except Exception as e:
            error("Failed to initialize RasterClipper",
                  component="initialization",
                  error=str(e))
            raise
    
    def _load_config(self, config_path: Path) -> Dict:
        """Load JSON configuration file"""
        try:
            with open(config_path) as f:
                config = json.load(f)
            info("Configuration loaded successfully",
                 component="config",
                 config_path=str(config_path))
            return config
        except Exception as e:
            error("Failed to load configuration file",
                  component="config",
                  config_path=str(config_path),
                  error=str(e))
            raise
    
    def _get_variable_mapping(self, config_path: Path) -> Dict:
        """Get variable mapping from a config file"""
        try:
            with open(config_path) as f:
                config = json.load(f)

            mapping = {}
            for dataset_name, dataset in config['datasets'].items():
                # General case: dataset contains multiple variables
                if 'variables' in dataset:
                    for var_name, var_config in dataset['variables'].items():
                        # Use 'output_dir' if available, fallback to 'file_name'
                        if 'output_dir' in var_config:
                            mapping[var_name] = var_config['output_dir']
                        elif 'file_name' in var_config:
                            mapping[var_name] = var_config['file_name']
                # Special case: single-variable dataset like CHIRPS
                elif 'output_dir' in dataset:
                    # Use the dataset name (e.g., 'CHIRPS') as the variable key
                    mapping[dataset_name] = dataset['output_dir']
            
            info("Variable mapping extracted",
                 component="config",
                 config_path=str(config_path),
                 variables=list(mapping.keys()))
            return mapping
            
        except Exception as e:
            error("Failed to extract variable mapping",
                  component="config",
                  config_path=str(config_path),
                  error=str(e))
            raise

    def _generate_output_name(self, var_name: str, date_str: str) -> str:
        """Generate output filename according to naming configuration"""
        try:
            components = self.naming_config['file_naming']['components']
            
            # Get variable code
            var_code = components['variable_mapping'].get(
                var_name, 
                var_name.lower()
            )
            
            # Get country code
            country_code = self.country_config['iso2_code']
            
            filename = self.naming_config['file_naming']['template'].format(
                temporal=components['temporal'],
                country=country_code,
                variable=var_code,
                date=date_str
            )
            
            info("Generated output filename",
                 component="processing",
                 variable=var_name,
                 date=date_str,
                 output_name=filename)
            return filename
            
        except Exception as e:
            error("Failed to generate output filename",
                  component="processing",
                  variable=var_name,
                  date=date_str,
                  error=str(e))
            raise

    def process_all(self, base_download_path: Path, base_processed_path: Path):
        """Process all downloaded data"""
        try:
            info("Starting raster clipping process",
                 component="processing",
                 base_download_path=str(base_download_path),
                 base_processed_path=str(base_processed_path))
            
            for downloader_name, config_path in self.downloader_configs.items():
                info(f"Processing data from downloader",
                     component="processing",
                     downloader_name=downloader_name)
                
                var_mapping = self._get_variable_mapping(config_path)

                for var_name, output_dir in var_mapping.items():
                    input_path = base_download_path / output_dir
                    output_path = base_processed_path / output_dir
                    
                    if not input_path.exists():
                        warning("Skipping variable - input path does not exist",
                                component="processing",
                                variable=var_name,
                                input_path=str(input_path))
                        continue
                    
                    self._process_variable(var_name, input_path, output_path)
            
            info("Raster clipping completed",
                 component="processing",
                 base_processed_path=str(base_processed_path))
                
        except Exception as e:
            error("Raster clipping process failed",
                  component="processing",
                  error=str(e))
            raise
    
    def _process_variable(self, var_name: str, input_path: Path, output_path: Path):
        """Process all files for a given variable"""
        try:
            info("Processing variable",
                 component="processing",
                 variable=var_name,
                 input_path=str(input_path),
                 output_path=str(output_path))
            
            files_processed = 0
            files_skipped = 0
            errors = 0
            
            for year_dir in input_path.glob("*"):
                if not year_dir.is_dir():
                    continue
                    
                output_year_path = output_path / year_dir.name
                output_year_path.mkdir(parents=True, exist_ok=True)
                
                for raster_file in year_dir.glob("*.tif"):
                    try:
                        result = self._process_raster(raster_file, var_name, output_year_path)
                        if result:
                            files_processed += 1
                        else:
                            files_skipped += 1
                    except Exception as e:
                        error("Failed to process raster file",
                              component="processing",
                              file=str(raster_file),
                              error=str(e))
                        errors += 1
            
            info("Variable processing completed",
                 component="processing",
                 variable=var_name,
                 files_processed=files_processed,
                 files_skipped=files_skipped,
                 errors=errors)
                
        except Exception as e:
            error("Failed to process variable",
                  component="processing",
                  variable=var_name,
                  error=str(e))
            raise

    def _process_raster(self, input_file: Path, var_name: str, output_dir: Path) -> bool:
        """Process a single raster file"""
        try:
            match = re.search(r'(\d{8})', input_file.stem)
            if not match:
                warning("No valid date found in filename",
                        component="processing",
                        file=input_file.name)
                return False

            date_str = match.group(1)  # e.g., "20200101"

            # Generate output name
            output_name = self._generate_output_name(var_name, date_str)
            output_file = output_dir / output_name

            if output_file.exists():
                info("Skipping existing output file",
                     component="processing",
                     output_file=str(output_file))
                return False

            info("Processing raster file",
                 component="processing",
                 input_file=str(input_file),
                 output_file=str(output_file))
            
            clipper = get_clipper(str(input_file), 'geoserver')
            clipper.connection = self.conn
            clipped = clipper.clip(
                self.country_config['geoserver']['workspace'],
                self.country_config['geoserver']['layer']
            )
            clipped.rio.to_raster(str(output_file))
            
            info("Raster processing completed",
                 component="processing",
                 output_file=str(output_file),
                 file_size=f"{output_file.stat().st_size/1024/1024:.2f}MB")
            return True
            
        except Exception as e:
            error("Failed to process raster file",
                  component="processing",
                  input_file=str(input_file),
                  error=str(e))
            raise

    def clean_processed_data(self, base_processed_path: Path, confirm: bool = False):
        """
        Deletes all processed raster files while maintaining the directory structure.
        
        Args:
            base_processed_path: Base path where processed data is stored
            confirm: If True, asks for confirmation before deletion (safety measure)
        """
        try:
            info("Starting processed data cleanup",
                 component="cleanup",
                 base_processed_path=str(base_processed_path),
                 confirm_required=confirm)
            
            if not base_processed_path.exists():
                warning("Processed data path does not exist",
                        component="cleanup",
                        path=str(base_processed_path))
                return
                
            total_files = sum(1 for _ in base_processed_path.glob('**/*.tif'))
            
            if total_files == 0:
                info("No raster files found to delete",
                     component="cleanup")
                return
                
            if confirm:
                response = input(f"Are you sure you want to delete {total_files} raster files in {base_processed_path}? [y/N]: ")
                if response.lower() != 'y':
                    info("Cleanup cancelled by user",
                         component="cleanup")
                    return
                    
            deleted_count = 0
            errors = 0
            
            for raster_file in base_processed_path.glob('**/*.tif'):
                try:
                    raster_file.unlink()
                    deleted_count += 1
                except Exception as e:
                    error("Failed to delete raster file",
                          component="cleanup",
                          file=str(raster_file),
                          error=str(e))
                    errors += 1
            
            info("Cleanup completed",
                 component="cleanup",
                 files_deleted=deleted_count,
                 total_files=total_files,
                 errors=errors)
                
        except Exception as e:
            error("Cleanup process failed",
                  component="cleanup",
                  error=str(e))
            raise