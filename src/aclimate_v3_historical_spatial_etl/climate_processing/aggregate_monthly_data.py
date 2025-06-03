import json
from pathlib import Path
import xarray as xr
import rioxarray
import numpy as np
from typing import Optional, List, Dict, Tuple, Union
from collections import defaultdict
import requests
from urllib.parse import urlencode
from xml.etree import ElementTree
import tempfile
import shutil
import os
import gc

class ClimatologyProcessor:
    def __init__(
        self,
        geoserver_workspace: str,
        geoserver_layer: str,
        geoserver_store: str,
        output_path: Union[str, Path],
        variable: str,
        naming_config_path: Union[str, Path],
        countries_config_path: Union[str, Path],
        country: Optional[str] = None,
        date_range: Optional[Tuple[str, str]] = None
    ):
        """
        Calculates climatology (monthly averages) for a given variable from GeoServer data.
        
        Args:
            geoserver_workspace: Workspace name in GeoServer (required)
            geoserver_layer: Layer name in GeoServer (required)
            geoserver_store: Store name in GeoServer (required)
            output_path: Path to save climatology results (required)
            variable: Variable name to process (required)
            naming_config_path: Path to naming configuration JSON file
            countries_config_path: Path to countries configuration JSON file
            country: Target country name (optional, uses default from config if not provided)
            date_range: Optional date range tuple (YYYY-MM, YYYY-MM) to limit the calculation
        """
        # Validate and get GeoServer configuration from environment
        self._validate_geoserver_envs()
        self.geoserver_url = os.getenv('GEOSERVER_URL').rstrip('/')
        self.geoserver_user = os.getenv('GEOSERVER_USER')
        self.geoserver_password = os.getenv('GEOSERVER_PASSWORD')
        
        # Validate required parameters
        if not geoserver_workspace:
            raise ValueError("geoserver_workspace parameter is required")
        if not geoserver_layer:
            raise ValueError("geoserver_layer parameter is required")
        if not geoserver_store:
            raise ValueError("geoserver_store parameter is required")
        if not variable:
            raise ValueError("variable parameter is required")
            
        self.geoserver_workspace = geoserver_workspace
        self.geoserver_layer = geoserver_layer
        self.geoserver_store = geoserver_store
        self.variable = variable
        
        # Convert paths to Path objects if they're strings
        naming_config_path = Path(naming_config_path) if isinstance(naming_config_path, str) else naming_config_path
        countries_config_path = Path(countries_config_path) if isinstance(countries_config_path, str) else countries_config_path
        self.output_path = Path(output_path) if isinstance(output_path, str) else output_path
        
        # Validate paths
        if not naming_config_path.exists():
            raise ValueError(f"Naming config file not found: {naming_config_path}")
        if not countries_config_path.exists():
            raise ValueError(f"Countries config file not found: {countries_config_path}")
        
        # Load configurations
        self._load_naming_config(naming_config_path)
        self._load_countries_config(countries_config_path, country)
        
        # Create output directory
        self.output_path = self.output_path / "climatology_data"
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        # Temporary directory for downloads
        self.temp_dir = Path(tempfile.mkdtemp())
        
        # Date range to process (None means use all available)
        self.date_range = date_range
        self._open_datasets = []

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

    def _generate_climatology_name(self, month: str) -> str:
        """
        Generates output filename for climatology data according to config template.
        
        Args:
            month: Month number (01-12)
            
        Returns:
            Generated filename
        """
        # Standardize variable name (handle spaces, hyphens, etc.)
        standardized_var = self.variable.replace(" ", "_").replace("-", "_")
        
        # Get standardized variable code from mapping
        var_code = self.variable_mapping.get(
            standardized_var,
            standardized_var.lower()
        )
        
        # Using fixed year (2000) for climatology files as per requirements
        return self.template.format(
            temporal="climatology",
            country=self.country_code,
            variable=var_code,
            date=f"2000{month}"  # Format as 200001, 200002, etc.
        )

    def _validate_geoserver_envs(self):
        """Validate required GeoServer environment variables"""
        required_envs = ['GEOSERVER_URL']
        missing_envs = [env for env in required_envs if not os.getenv(env)]
        
        if missing_envs:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_envs)}. "
                "Please set these variables before initializing the ClimatologyCalculator."
            )
        
        # Check if URL is valid
        geoserver_url = os.getenv('GEOSERVER_URL')
        if not geoserver_url:
            raise ValueError("GEOSERVER_URL environment variable is empty")
        
        # Check if credentials are provided (either both or none)
        has_user = bool(os.getenv('GEOSERVER_USER'))
        has_password = bool(os.getenv('GEOSERVER_PASSWORD'))
        
        if has_user != has_password:
            raise ValueError(
                "Both GEOSERVER_USER and GEOSERVER_PASSWORD must be provided together "
                "or both omitted for anonymous access"
            )
        
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def cleanup(self):
        """Explicit cleanup method to close resources"""
        # Close all open datasets
        for ds in self._open_datasets:
            try:
                ds.close()
            except:
                pass
        self._open_datasets.clear()
        
        # Clean up temp directory with error handling
        if hasattr(self, 'temp_dir') and self.temp_dir.exists():
            try:
                # Try to remove files one by one first
                for root, dirs, files in os.walk(self.temp_dir, topdown=False):
                    for name in files:
                        try:
                            os.unlink(os.path.join(root, name))
                        except Exception as e:
                            print(f"Warning: Could not delete {name}: {str(e)}")
                    for name in dirs:
                        try:
                            os.rmdir(os.path.join(root, name))
                        except Exception as e:
                            print(f"Warning: Could not delete directory {name}: {str(e)}")
                
                # Final attempt to remove the directory
                try:
                    shutil.rmtree(self.temp_dir)
                except Exception as e:
                    print(f"Warning: Could not fully clean temp directory: {str(e)}")
            except Exception as e:
                print(f"Error during cleanup: {str(e)}")

    def __del__(self):
        """Clean up temporary directory when object is destroyed"""
        self.cleanup()

    def calculate_climatology(self) -> Dict[str, Path]:
        """
        Calculates monthly climatology for the configured variable.
        
        Returns:
            Dictionary mapping month numbers (as strings '01'-'12') to output file paths
        """
        # Get available dates from GeoServer
        all_dates = self.get_dates_from_geoserver()
        print(f"Total available dates in GeoServer: {len(all_dates)}")
        
        # Calculate date range statistics
        years = sorted(list({d.split('-')[0] for d in all_dates}))
        print(f"Data covers years: {min(years)} to {max(years)} ({len(years)} years)")
        
        # Filter dates by range if specified
        if self.date_range:
            start_date, end_date = self.date_range
            filtered_dates = [d for d in all_dates if start_date <= d <= end_date]
            if not filtered_dates:
                raise ValueError(f"No dates available in the specified range {start_date} to {end_date}")
            dates_to_process = filtered_dates
            print(f"Filtered to {len(dates_to_process)} dates in range {start_date} to {end_date}")
        else:
            dates_to_process = all_dates
        
        print(f"\nProcessing {len(dates_to_process)} monthly records for climatology")
        
        # Organize files by month (key: '01'-'12')
        monthly_data = defaultdict(list)
        download_errors = 0
        processing_errors = 0
        
        # Download and process each month
        for i, date_str in enumerate(dates_to_process, 1):
            month = date_str.split('-')[1]  # Extract month number
            year = date_str.split('-')[0]
            
            print(f"\nProcessing {date_str} ({i}/{len(dates_to_process)})...")
            
            # Download the GeoTIFF
            file_path = self._download_from_geoserver(date_str)
            if file_path is None:
                print(f"Warning: Failed to download data for {date_str}")
                download_errors += 1
                continue
            
            try:
                # Open the raster file and track it
                print(f"Opening dataset for {date_str}")
                ds = rioxarray.open_rasterio(file_path)
                self._open_datasets.append(ds)
                
                monthly_data[month].append(ds)
                print(f"Added {date_str} to month {month} processing")
            except Exception as e:
                print(f"Error processing {date_str}: {str(e)}")
                processing_errors += 1
                continue
        
        # Print summary before calculations
        print("\nMonthly data summary:")
        for month in sorted(monthly_data.keys()):
            print(f"Month {month}: {len(monthly_data[month])} years of data")
        
        print(f"\nTotal download errors: {download_errors}")
        print(f"Total processing errors: {processing_errors}")
        
        # Calculate climatology for each month
        climatology_results = {}
        
        for month in sorted(monthly_data.keys()):
            year_count = len(monthly_data[month])
            print(f"\nCalculating climatology for month {month} (using {year_count} years)...")
            
            # Combine all years for this month
            combined = xr.concat(monthly_data[month], dim='time')
            
            # Calculate mean across all years for this month
            monthly_mean = combined.mean(dim='time', skipna=True)
            
            # Generate output filename
            output_filename = self._generate_climatology_name(month)
            output_path = self.output_path / self.geoserver_store / output_filename
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            print(f"Saving climatology for month {month} to {output_path}")
            monthly_mean.rio.to_raster(output_path)
            
            climatology_results[month] = output_path
            
            # Clean up
            combined.close()
            monthly_mean.close()
        
        # Explicit cleanup
        self.cleanup()
        gc.collect()
        
        return climatology_results

    def _download_from_geoserver(self, date_str: str) -> Optional[Path]:
        """
        Downloads a monthly GeoTIFF file from GeoServer using WCS.
        
        Args:
            date_str: Date string in "YYYY-MM" format
            
        Returns:
            Path to the downloaded file, or None if the download fails.
        """
        year_month = date_str.replace("-", "")
        output_filename = f"{self.variable}_{year_month}.tif"
        output_dir = self.temp_dir / "downloads"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / output_filename

        try:
            # Build WCS request parameters
            params = {
                "service": "WCS",
                "version": "2.0.1",
                "request": "GetCoverage",
                "coverageId": self.geoserver_layer,
                "subset": f"time(\"{date_str}-01T00:00:00.000Z\")",
                "format": "image/geotiff"
            }
            base_url = f"{self.geoserver_url}/{self.geoserver_workspace}/ows?"
            url = base_url + urlencode(params)

            auth = (self.geoserver_user, self.geoserver_password) if self.geoserver_user and self.geoserver_password else None
            response = requests.get(url, auth=auth, stream=True, timeout=60)
            response.raise_for_status()

            # Validate GeoTIFF header
            if not response.content.startswith(b'\x49\x49\x2A\x00') and not response.content.startswith(b'\x4D\x4D\x00\x2A'):
                raise ValueError("Downloaded file is not a valid GeoTIFF")

            # Save file
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            return output_path

        except Exception as e:
            print(f"Warning: Failed to download {self.variable} for {date_str}: {str(e)}")
            if output_path.exists():
                output_path.unlink()
            return None

    def get_dates_from_geoserver(self) -> List[str]:
        """
        Retrieves available dates from GeoServer for the configured layer.
        
        Returns:
            List of dates in YYYY-MM format
        """
        url = f"{self.geoserver_url}/{self.geoserver_workspace}/wms?service=WMS&version=1.3.0&request=GetCapabilities"

        try:
            # Use basic auth if credentials are provided
            auth = (self.geoserver_user, self.geoserver_password) if self.geoserver_user and self.geoserver_password else None
            
            response = requests.get(url, auth=auth, timeout=30)
            response.raise_for_status()
            
            # Check if we got a valid XML response
            if not response.content.strip().startswith(b'<?xml'):
                raise ValueError("Invalid response from GeoServer - not XML")
            
            namespaces = {'wms': 'http://www.opengis.net/wms'}
            root = ElementTree.fromstring(response.content)
            
            # Find the layer with our target name
            for layer in root.findall('.//wms:Layer', namespaces):
                name_elem = layer.find('wms:Name', namespaces)
                if name_elem is not None and name_elem.text == self.geoserver_store:
                    dimension_elem = layer.find('wms:Dimension', namespaces)
                    if dimension_elem is not None:
                        # Parse the time dimension values
                        time_values = dimension_elem.text.split(',')
                        # Extract YYYY-MM format
                        dates = [t.split('T')[0][:7] for t in time_values if t.strip()]
                        return sorted(list(set(dates)))  # Remove duplicates and sort
            
            raise ValueError(f"Store '{self.geoserver_store}' not found in GeoServer capabilities or has no time dimension")
            
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"GeoServer request failed: {str(e)}")
        except ElementTree.ParseError as e:
            raise RuntimeError(f"Failed to parse GeoServer response: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"Error retrieving dates from GeoServer: {str(e)}")