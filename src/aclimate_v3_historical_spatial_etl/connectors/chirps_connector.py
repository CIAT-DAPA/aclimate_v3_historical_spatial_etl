import os
import urllib.request
import gzip
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from ..tools import DownloadProgressBar, Tools

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
        self.config = self._load_config(config_path)
        self.download_data_path = Path(download_data_path)
        self.start_date = start_date
        self.end_date = end_date
        
        self.tools = Tools()
        self.cores = self.config.get('parallel_downloads', 4)
        
        # Initialize paths
        self._initialize_paths()
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from JSON file"""
        with open(config_path) as f:
            return json.load(f)
    
    def _initialize_paths(self):
        """Create necessary directory structure"""
        chirps_config = self.config['datasets']['CHIRPS']
        
        # Download paths
        self.chirps_path = self.download_data_path / chirps_config['output_dir']
        self.chirps_path.mkdir(parents=True, exist_ok=True)
        
        # Shapefile paths
        self.project_root = Path(__file__).parents[2]
        self.shapefile_path = self.project_root / "shapefiles"
    
    def _build_download_url(self, date: str) -> str:
        """Construct download URL from configuration and date"""
        url_config = self.config['datasets']['CHIRPS']['url_config']
        return (
            f"{url_config['base_url']}"
            f"{date.split('-')[0]}/"
            f"{url_config['file_pattern'].replace('date', date.replace('-', '.'))}"
        )
    
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
            print(f"\tFile already exists: {uncompressed_path}")
            return
        
        # Download the compressed file
        with DownloadProgressBar(unit='B', unit_scale=True, miniters=1, 
                               desc=url.split('/')[-1]) as t:
            urllib.request.urlretrieve(url, filename=path, reporthook=t.update_to)
        
        # Decompress the file
        try:
            with gzip.open(path, 'rb') as f_in:
                with open(uncompressed_path, 'wb') as f_out:
                    f_out.write(f_in.read())
            
            if remove_compressed:
                path.unlink()
                
        except Exception as e:
            print(f"\tError processing {path}: {str(e)}")
            if uncompressed_path.exists():
                uncompressed_path.unlink()
    
    def download_data(self):
        """
        Download CHIRPS data for the specified date range in parallel.
        Returns list of downloaded file paths.
        """
        dates = self.tools.generate_dates(self.start_date, self.end_date)
        download_paths = []
        
        # Prepare download tasks
        for date in dates:
            year = date.split('-')[0]
            year_path = self.chirps_path / year
            year_path.mkdir(exist_ok=True)
            
            url = self._build_download_url(date)
            filename = os.path.basename(url)
            save_path = year_path / filename
            download_paths.append(save_path)
        
        # Execute downloads in parallel
        with ThreadPoolExecutor(max_workers=self.cores) as executor:
            urls = [self._build_download_url(date) for date in dates]
            executor.map(self._download_file, urls, download_paths)
        
        return [p.with_suffix('') for p in download_paths]  # Return uncompressed paths
    

    def main(self):
        """Main processing pipeline"""
        self.download_data()