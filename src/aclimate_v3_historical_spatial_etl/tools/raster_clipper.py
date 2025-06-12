import json
from pathlib import Path
from typing import Dict, Any
from aclimate_v3_cut_spatial_data import get_clipper, GeoServerBasicAuth
import re

class RasterClipper:
    def __init__(self, 
                 country: str,
                 downloader_configs: Dict[str, Path],
                 naming_config_path: Path,
                 clipping_config_path: Path):
        """
        Args:
            country: Target country (e.g., "COLOMBIA")
            downloader_configs: Dictionary with {'downloader_name': Path(config_file)}
            naming_config_path: Path to the naming config file
            clipping_config_path: Path to the clipping config file
        """
        self.country = country.upper()
        self.downloader_configs = downloader_configs
        
        # Load configurations
        self.naming_config = self._load_config(naming_config_path)
        self.clipping_config = self._load_config(clipping_config_path)
        
        # Validate country
        if self.country not in self.clipping_config['countries']:
            raise ValueError(f"Country '{country}' not found in configuration")
        
        self.country_config = self.clipping_config['countries'][self.country]
        self.conn = GeoServerBasicAuth()
    
    def _load_config(self, config_path: Path) -> Dict:
        """Load JSON configuration file"""
        with open(config_path) as f:
            return json.load(f)
    
    def _get_variable_mapping(self, config_path: Path) -> Dict:
        """Get variable mapping from a config file"""
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
        
        return mapping

    
    def _generate_output_name(self, var_name: str, date_str: str) -> str:

        components = self.naming_config['file_naming']['components']
        
        # Get variable code
        var_code = components['variable_mapping'].get(
            var_name, 
            var_name.lower()
        )
        
        # Get country code
        country_code = self.country_config['iso2_code']
        
        return self.naming_config['file_naming']['template'].format(
            temporal=components['temporal'],
            country=country_code,
            variable=var_code,
            date=date_str
        )
    
    def process_all(self, base_download_path: Path, base_processed_path: Path):
        """Process all downloaded data"""
        for downloader_name, config_path in self.downloader_configs.items():
            print(f"\nProcessing data from {downloader_name}...")
            
            var_mapping = self._get_variable_mapping(config_path)

            for var_name, output_dir in var_mapping.items():
                input_path = base_download_path / output_dir
                output_path = base_processed_path / output_dir
                
                if not input_path.exists():
                    print(f"  Skipping {var_name} - no data available")
                    continue
                
                self._process_variable(var_name, input_path, output_path)
    
    def _process_variable(self, var_name: str, input_path: Path, output_path: Path):
        """Process all files for a given variable"""
        for year_dir in input_path.glob("*"):
            if not year_dir.is_dir():
                continue
                
            output_year_path = output_path / year_dir.name
            output_year_path.mkdir(parents=True, exist_ok=True)
            
            for raster_file in year_dir.glob("*.tif"):
                self._process_raster(raster_file, var_name, output_year_path)
    
    def _process_raster(self, input_file: Path, var_name: str, output_dir: Path):
        """Process a single raster file"""
        match = re.search(r'(\d{8})', input_file.stem)
        if not match:
            print(f"  No valid date found in file name: {input_file.name}")
            return

        date_str = match.group(1)  # e.g., "20200101"

        # Generate output name
        output_name = self._generate_output_name(var_name, date_str)
        output_file = output_dir / output_name

        if output_file.exists():
            print(f"  File already exists: {output_file}")
            return

        try:
            print(f"  Processing: {input_file} -> {output_file}")
            clipper = get_clipper(str(input_file), 'geoserver')
            clipper.connection = self.conn
            clipped = clipper.clip(
                self.country_config['geoserver']['workspace'],
                self.country_config['geoserver']['layer']
            )
            clipped.rio.to_raster(str(output_file))

        except Exception as e:
            print(f"  Error processing {input_file}: {str(e)}")

    def clean_processed_data(self, base_processed_path: Path, confirm: bool = False):
        """
        Deletes all processed raster files while maintaining the directory structure.
        
        Args:
            base_processed_path: Base path where processed data is stored
            confirm: If True, asks for confirmation before deletion (safety measure)
        """
        if not base_processed_path.exists():
            print(f"Processed data path does not exist: {base_processed_path}")
            return
            
        total_files = sum(1 for _ in base_processed_path.glob('**/*.tif'))
        
        if total_files == 0:
            print("No raster files to delete")
            return
            
        if confirm:
            response = input(f"Are you sure you want to delete {total_files} raster files in {base_processed_path}? [y/N]: ")
            if response.lower() != 'y':
                print("Operation cancelled")
                return
                
        deleted_count = 0
        for raster_file in base_processed_path.glob('**/*.tif'):
            try:
                raster_file.unlink()
                deleted_count += 1
            except Exception as e:
                print(f"Error deleting {raster_file}: {str(e)}")
                
        print(f"Deleted {deleted_count}/{total_files} raster files")