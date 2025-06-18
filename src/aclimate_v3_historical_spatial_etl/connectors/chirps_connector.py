import os
import urllib.request
import gzip
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from ..tools import DownloadProgressBar, Tools, error, info, warning
from typing import List

class ChirpsDownloader:
    def __init__(self, config_path: str,
                 start_date: str, end_date: str, download_data_path: str):
        """
        CHIRPS data downloader and processor with configuration support.
        
        Args:
            config_path: Path to configuration JSON file
            output_path: Base output directory for processed files
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            download_data_path: Temporary download directory
        """
        try:
            self.config = self._load_config(config_path)
            self.download_data_path = Path(download_data_path)
            self.start_date = start_date
            self.end_date = end_date
            
            self.tools = Tools()
            self.cores = self.config.get('parallel_downloads', 4)
            
            # Initialize paths
            self._initialize_paths()
            
            info("CHIRPS Downloader initialized successfully",
                 component="downloader",
                 config_path=config_path,
                 date_range=f"{start_date} to {end_date}",
                 download_path=str(download_data_path),
                 parallel_cores=self.cores)
        except Exception as e:
            error("Failed to initialize CHIRPS Downloader",
                  component="initialization",
                  error=str(e))
            raise
    
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from JSON file"""
        try:
            with open(config_path) as f:
                config = json.load(f)
            info("Configuration loaded successfully",
                 component="config",
                 config_path=config_path)
            return config
        except Exception as e:
            error("Failed to load configuration",
                  component="config",
                  config_path=config_path,
                  error=str(e))
            raise
    
    def _initialize_paths(self):
        """Create necessary directory structure"""
        try:
            chirps_config = next(iter(self.config['datasets'].values()))
            
            # Download paths
            self.chirps_path = self.download_data_path / chirps_config['output_dir']
            self.chirps_path.mkdir(parents=True, exist_ok=True)
            
            # Shapefile paths
            self.project_root = Path(__file__).parents[2]
            self.shapefile_path = self.project_root / "shapefiles"
            
            info("Directory structure initialized",
                 component="setup",
                 chirps_path=str(self.chirps_path),
                 shapefile_path=str(self.shapefile_path))
        except Exception as e:
            error("Failed to initialize directory structure",
                  component="setup",
                  error=str(e))
            raise
    
    def _build_download_url(self, date: str) -> str:
        """Construct download URL from configuration and date"""
        try:
            chirps_config = next(iter(self.config['datasets'].values()))
            url_config = chirps_config['url_config']
            url = (
                f"{url_config['base_url']}"
                f"{date.split('-')[0]}/"
                f"{url_config['file_pattern'].replace('date', date.replace('-', '.'))}"
            )
            info("URL constructed",
                 component="download",
                 date=date,
                 url=url)
            return url
        except Exception as e:
            error("Failed to build download URL",
                  component="download",
                  date=date,
                  error=str(e))
            raise
    
    def _download_file(self, url: str, path: Path, remove_compressed: bool = True):
        """
        Download and decompress a single file.
        
        Args:
            url: Source URL to download from
            path: Destination path (including .gz extension)
            remove_compressed: Whether to remove the .gz file after extraction
        """
        uncompressed_path = path.with_suffix('')  # Remove .gz extension
        
        if uncompressed_path.exists():
            info("File already exists - skipping download",
                 component="download",
                 file=str(uncompressed_path))
            return
        
        try:
            # Download the compressed file
            info("Starting file download",
                 component="download",
                 url=url,
                 target=str(path))
            
            with DownloadProgressBar(unit='B', unit_scale=True, miniters=1, 
                                   desc=url.split('/')[-1]) as t:
                urllib.request.urlretrieve(url, filename=path, reporthook=t.update_to)
            
            # Decompress the file
            info("Decompressing file",
                 component="download",
                 file=str(path))
            
            with gzip.open(path, 'rb') as f_in:
                with open(uncompressed_path, 'wb') as f_out:
                    f_out.write(f_in.read())
            
            if remove_compressed:
                path.unlink()
                info("Compressed file removed",
                     component="cleanup",
                     file=str(path))
            
            info("File download and decompression completed",
                 component="download",
                 file=str(uncompressed_path),
                 size=f"{uncompressed_path.stat().st_size/1024/1024:.2f}MB")
                
        except Exception as e:
            error("File download failed",
                  component="download",
                  url=url,
                  target=str(path),
                  error=str(e))
            
            if uncompressed_path.exists():
                try:
                    uncompressed_path.unlink()
                    warning("Removed partially downloaded file",
                            component="cleanup",
                            file=str(uncompressed_path))
                except Exception as cleanup_error:
                    error("Failed to remove partially downloaded file",
                          component="cleanup",
                          file=str(uncompressed_path),
                          error=str(cleanup_error))
    
    def download_data(self) -> List[Path]:
        """
        Download CHIRPS data for the specified date range in parallel.
        Files are saved using the naming format defined in config.
        Returns list of downloaded file paths (uncompressed).
        """
        try:
            info("Starting CHIRPS data download",
                 component="download",
                 date_range=f"{self.start_date} to {self.end_date}",
                 parallel_cores=self.cores)
            
            dates = self.tools.generate_dates(self.start_date, self.end_date)
            download_paths = []
            chirps_config = next(iter(self.config['datasets'].values()))
            var_name = chirps_config['output_dir']  # e.g., "Precipitation"
            naming_template = chirps_config.get('file_naming', "{variable}_{date}.tif.gz")
            
            for date in dates:
                year = date.split('-')[0]
                year_path = self.chirps_path / year
                year_path.mkdir(exist_ok=True)
                
                url = self._build_download_url(date)
                
                # Format date for filename (remove dashes)
                date_str = date.replace('-', '')  # e.g., '20200120'
                
                # Use naming template from config
                filename = naming_template.format(variable=var_name, date=date_str)
                
                save_path = year_path / filename
                download_paths.append(save_path)
            
            # Download in parallel
            info(f"Starting parallel download of {len(download_paths)} files",
                 component="download",
                 parallel_cores=self.cores)
            
            with ThreadPoolExecutor(max_workers=self.cores) as executor:
                urls = [self._build_download_url(date) for date in dates]
                executor.map(self._download_file, urls, download_paths)
            
            uncompressed_paths = [p.with_suffix('') for p in download_paths]
            
            info("CHIRPS data download completed",
                 component="download",
                 files_downloaded=len(uncompressed_paths))
            
            return uncompressed_paths
            
        except Exception as e:
            error("CHIRPS data download failed",
                  component="download",
                  error=str(e))
            raise
    
    def main(self):
        """Main processing pipeline"""
        try:
            info("Starting CHIRPS downloader main pipeline", component="main")
            result = self.download_data()
            info("CHIRPS downloader pipeline completed successfully",
                 component="main",
                 files_processed=len(result))
            return result
        except Exception as e:
            error("CHIRPS downloader pipeline failed",
                  component="main",
                  error=str(e))
            raise