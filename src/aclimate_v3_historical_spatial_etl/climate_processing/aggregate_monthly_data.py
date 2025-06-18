import json
from pathlib import Path
import xarray as xr
import rioxarray
import numpy as np
from typing import Optional, List, Dict, Tuple, Union
from ..tools import error, warning, info
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
        try:
            # Validate and get GeoServer configuration from environment
            self._validate_geoserver_envs()
            self.geoserver_url = os.getenv('GEOSERVER_URL').rstrip('/')

            if self.geoserver_url.endswith('/rest'):
                self.geoserver_url = self.geoserver_url[:-5]
            else:
                self.geoserver_url = self.geoserver_url.replace('/rest/', '/').rstrip('/')

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
            
            # Convert paths to Path objects
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
            self.output_path.mkdir(parents=True, exist_ok=True)
            
            # Temporary directory for downloads
            self.temp_dir = Path(tempfile.mkdtemp())
            
            # Date range to process
            self.date_range = date_range
            self._open_datasets = []

            info("ClimatologyProcessor initialized successfully",
                 component="initialization",
                 geoserver_workspace=geoserver_workspace,
                 geoserver_layer=geoserver_layer,
                 variable=variable,
                 output_path=str(self.output_path))
                
        except Exception as e:
            error("Failed to initialize ClimatologyProcessor",
                  component="initialization",
                  error=str(e))
            raise

    def _load_naming_config(self, config_path: Path):
        """Load naming configuration from JSON file"""
        try:
            with open(config_path) as f:
                self.naming_config = json.load(f)
            
            self.template = self.naming_config['file_naming']['template']
            self.variable_mapping = self.naming_config['file_naming']['components']['variable_mapping']

            info("Naming configuration loaded",
                 component="config",
                 config_path=str(config_path),
                 template=self.template)
                
        except Exception as e:
            error("Failed to load naming configuration",
                  component="config",
                  config_path=str(config_path),
                  error=str(e))
            raise

    def _load_countries_config(self, config_path: Path, country: Optional[str]):
        """Load country configuration and set country code"""
        try:
            with open(config_path) as f:
                countries_config = json.load(f)
            
            target_country = country or countries_config.get('default_country')
            if not target_country:
                raise ValueError("No country specified and no default country in config")
                
            country_config = countries_config['countries'].get(target_country.upper())
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
                  config_path=str(config_path),
                  error=str(e))
            raise

    def _generate_climatology_name(self, month: str) -> str:
        """
        Generates output filename for climatology data according to config template.
        
        Args:
            month: Month number (01-12)
            
        Returns:
            Generated filename
        """
        try:
            standardized_var = self.variable.replace(" ", "_").replace("-", "_")
            var_code = self.variable_mapping.get(standardized_var, standardized_var.lower())
            
            filename = self.template.format(
                temporal="climatology",
                country=self.country_code,
                variable=var_code,
                date=f"2000{month}"  # Format as 200001, 200002, etc.
            )

            info("Generated climatology filename",
                 component="processing",
                 month=month,
                 output_name=filename)
                
            return filename
            
        except Exception as e:
            error("Failed to generate climatology filename",
                  component="processing",
                  month=month,
                  error=str(e))
            raise

    def _validate_geoserver_envs(self):
        """Validate required GeoServer environment variables"""
        try:
            required_envs = ['GEOSERVER_URL']
            missing_envs = [env for env in required_envs if not os.getenv(env)]
            
            if missing_envs:
                raise ValueError(f"Missing required environment variables: {', '.join(missing_envs)}")
            
            geoserver_url = os.getenv('GEOSERVER_URL')
            if not geoserver_url:
                raise ValueError("GEOSERVER_URL environment variable is empty")
            
            has_user = bool(os.getenv('GEOSERVER_USER'))
            has_password = bool(os.getenv('GEOSERVER_PASSWORD'))
            
            if has_user != has_password:
                raise ValueError("Both GEOSERVER_USER and GEOSERVER_PASSWORD must be provided together or both omitted")

            info("GeoServer environment variables validated",
                 component="config",
                 geoserver_url=geoserver_url,
                 has_credentials=has_user and has_password)
                
        except Exception as e:
            error("GeoServer environment validation failed",
                  component="config",
                  error=str(e))
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        try:
            # Close all open datasets
            for ds in self._open_datasets:
                try:
                    ds.close()
                except Exception as e:
                    warning("Failed to close dataset",
                            component="cleanup",
                            error=str(e))
            self._open_datasets.clear()
            
            # Clean up temp directory
            if hasattr(self, 'temp_dir') and self.temp_dir.exists():
                try:
                    shutil.rmtree(self.temp_dir)
                    info("Temporary directory cleaned",
                         component="cleanup",
                         path=str(self.temp_dir))
                except Exception as e:
                    warning("Failed to fully clean temporary directory",
                            component="cleanup",
                            path=str(self.temp_dir),
                            error=str(e))
                    
        except Exception as e:
            error("Cleanup failed",
                  component="cleanup",
                  error=str(e))
            raise

    def calculate_climatology(self) -> Dict[str, Path]:
        """
        Calculates monthly climatology for the configured variable.
        
        Returns:
            Dictionary mapping month numbers (as strings '01'-'12') to output file paths
        """
        try:
            info("Starting climatology calculation",
                 component="processing",
                 variable=self.variable,
                 date_range=self.date_range)
            
            # Get available dates from GeoServer
            all_dates = self.get_dates_from_geoserver()
            info("Retrieved available dates from GeoServer",
                 component="geoserver",
                 date_count=len(all_dates))
            
            # Calculate date range statistics
            years = sorted(list({d.split('-')[0] for d in all_dates}))
            info("Date range statistics",
                 component="processing",
                 min_year=min(years),
                 max_year=max(years),
                 year_count=len(years))
            
            # Filter dates by range if specified
            if self.date_range:
                start_date, end_date = self.date_range
                filtered_dates = [d for d in all_dates if start_date <= d <= end_date]
                if not filtered_dates:
                    raise ValueError(f"No dates available in specified range {start_date} to {end_date}")
                dates_to_process = filtered_dates
                info("Filtered dates by range",
                     component="processing",
                     date_range=f"{start_date} to {end_date}",
                     filtered_count=len(dates_to_process))
            else:
                dates_to_process = all_dates
            
            info("Processing monthly records for climatology",
                 component="processing",
                 record_count=len(dates_to_process))
            
            # Organize files by month
            monthly_data = defaultdict(list)
            download_errors = 0
            processing_errors = 0
            
            for i, date_str in enumerate(dates_to_process, 1):
                month = date_str.split('-')[1]
                year = date_str.split('-')[0]
                
                info(f"Processing date {i}/{len(dates_to_process)}",
                     component="processing",
                     date=date_str,
                     month=month,
                     year=year)
                
                # Download the GeoTIFF
                file_path = self._download_from_geoserver(date_str)
                if file_path is None:
                    warning("Failed to download data",
                            component="download",
                            date=date_str)
                    download_errors += 1
                    continue
                
                try:
                    ds = rioxarray.open_rasterio(file_path)
                    self._open_datasets.append(ds)
                    monthly_data[month].append(ds)
                    
                    info("Added date to monthly processing",
                         component="processing",
                         date=date_str,
                         month=month)
                         
                except Exception as e:
                    warning("Failed to process date",
                            component="processing",
                            date=date_str,
                            error=str(e))
                    processing_errors += 1
                    continue
            
            # Log monthly data summary
            for month in sorted(monthly_data.keys()):
                info("Monthly data summary",
                     component="processing",
                     month=month,
                     year_count=len(monthly_data[month]))
            
            info("Processing error summary",
                 component="processing",
                 download_errors=download_errors,
                 processing_errors=processing_errors)
            
            # Calculate climatology for each month
            climatology_results = {}
            
            for month in sorted(monthly_data.keys()):
                year_count = len(monthly_data[month])
                info("Calculating monthly climatology",
                     component="processing",
                     month=month,
                     year_count=year_count)
                
                try:
                    combined = xr.concat(monthly_data[month], dim='time')
                    monthly_mean = combined.mean(dim='time', skipna=True)
                    
                    output_filename = self._generate_climatology_name(month)
                    output_path = self.output_path / self.variable / output_filename
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    monthly_mean.rio.to_raster(output_path)
                    climatology_results[month] = output_path
                    
                    info("Saved monthly climatology",
                         component="processing",
                         month=month,
                         output_path=str(output_path))
                    
                    combined.close()
                    monthly_mean.close()
                    
                except Exception as e:
                    error("Failed to calculate monthly climatology",
                          component="processing",
                          month=month,
                          error=str(e))
                    continue
            
            info("Climatology calculation completed",
                 component="processing",
                 months_processed=len(climatology_results),
                 output_directory=str(self.output_path))
            
            return climatology_results
            
        except Exception as e:
            error("Climatology calculation failed",
                  component="processing",
                  error=str(e))
            raise
        finally:
            self.cleanup()
            gc.collect()

    def _download_from_geoserver(self, date_str: str) -> Optional[Path]:
        """
        Downloads a monthly GeoTIFF file from GeoServer using WCS.
        
        Args:
            date_str: Date string in "YYYY-MM" format
            
        Returns:
            Path to the downloaded file, or None if the download fails.
        """
        try:
            year_month = date_str.replace("-", "")
            output_filename = f"{self.variable}_{year_month}.tif"
            output_dir = self.temp_dir / "downloads"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / output_filename

            info("Initiating GeoServer download",
                 component="download",
                 date=date_str,
                 target_path=str(output_path))
            
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

            info("GeoServer download completed",
                 component="download",
                 date=date_str,
                 file_size=f"{output_path.stat().st_size/1024/1024:.2f}MB")
            
            return output_path

        except Exception as e:
            warning("GeoServer download failed",
                    component="download",
                    date=date_str,
                    error=str(e))
            if output_path.exists():
                try:
                    output_path.unlink()
                except Exception as e:
                    warning("Failed to remove incomplete download",
                            component="cleanup",
                            path=str(output_path),
                            error=str(e))
            return None

    def get_dates_from_geoserver(self) -> List[str]:
        """
        Retrieves available dates from GeoServer for the configured layer.
        
        Returns:
            List of dates in YYYY-MM format
        """
        try:
            url = f"{self.geoserver_url}/{self.geoserver_workspace}/wms?service=WMS&version=1.3.0&request=GetCapabilities"
            info("Requesting GeoServer capabilities",
                 component="geoserver",
                 url=url)

            auth = (self.geoserver_user, self.geoserver_password) if self.geoserver_user and self.geoserver_password else None
            response = requests.get(url, auth=auth, timeout=30)
            response.raise_for_status()
            
            if not response.content.strip().startswith(b'<?xml'):
                raise ValueError("Invalid response from GeoServer - not XML")
            
            namespaces = {'wms': 'http://www.opengis.net/wms'}
            root = ElementTree.fromstring(response.content)
            
            for layer in root.findall('.//wms:Layer', namespaces):
                name_elem = layer.find('wms:Name', namespaces)
                if name_elem is not None and name_elem.text == self.geoserver_store:
                    dimension_elem = layer.find('wms:Dimension', namespaces)
                    if dimension_elem is not None:
                        time_values = dimension_elem.text.split(',')
                        dates = [t.split('T')[0][:7] for t in time_values if t.strip()]
                        unique_dates = sorted(list(set(dates)))
                        
                        info("Retrieved dates from GeoServer",
                             component="geoserver",
                             date_count=len(unique_dates),
                             example_dates=unique_dates[:3] if unique_dates else None)
                            
                        return unique_dates
            
            raise ValueError(f"Store '{self.geoserver_store}' not found or has no time dimension")
            
        except Exception as e:
            error("Failed to retrieve dates from GeoServer",
                  component="geoserver",
                  error=str(e))
            raise