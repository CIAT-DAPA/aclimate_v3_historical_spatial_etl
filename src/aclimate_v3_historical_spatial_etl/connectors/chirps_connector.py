import os
import urllib.request
import gzip
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from ..tools import DownloadProgressBar, Tools, error, info, warning
from typing import List, Dict

class ChirpsDownloader:
    def __init__(self, config: Dict,
                 start_date: str, end_date: str, 
                 download_data_path: str):
        """
        CHIRPS data downloader and processor.
        
        Args:
            config: dict with chirps configuration
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            download_data_path: Temporary download directory
        """
        self.config = config
        self.download_data_path = Path(download_data_path)
        self.start_date = start_date
        self.end_date = end_date
        self.tools = Tools()
        self.cores = self.config.get('parallel_downloads', 4)
        
        self._initialize_paths()
        
        info(f"CHIRPS Downloader initialized {start_date} to {end_date}",
             component="downloader",
             date_range=f"{start_date} to {end_date}")

    def _initialize_paths(self):
        """Create necessary directory structure"""
        chirps_config = next(iter(self.config['datasets'].values()))
        self.chirps_path = self.download_data_path / chirps_config['output_dir']
        self.chirps_path.mkdir(parents=True, exist_ok=True)
        
        self.project_root = Path(__file__).parents[2]
        self.shapefile_path = self.project_root / "shapefiles"

    def _build_download_url(self, date: str) -> str:
        """Construct download URL from configuration and date"""
        chirps_config = next(iter(self.config['datasets'].values()))
        url_config = chirps_config['url_config']
        return (f"{url_config['base_url']}"
                f"{date.split('-')[0]}/"
                f"{url_config['file_pattern'].replace('date', date.replace('-', '.'))}")

    def _download_file(self, url: str, path: Path, remove_compressed: bool = True):
        """Download and decompress a single file"""
        uncompressed_path = path.with_suffix('')
        
        if uncompressed_path.exists():
            info("File exists, skipping", component="download", file=str(uncompressed_path))
            return

        try:
            with DownloadProgressBar(unit='B', unit_scale=True, miniters=1, 
                                   desc=url.split('/')[-1]) as t:
                urllib.request.urlretrieve(url, filename=path, reporthook=t.update_to)

            with gzip.open(path, 'rb') as f_in:
                with open(uncompressed_path, 'wb') as f_out:
                    f_out.write(f_in.read())

            if remove_compressed:
                path.unlink()

        except Exception as e:
            error("Download failed", component="download", url=url, error=str(e))
            if uncompressed_path.exists():
                try:
                    uncompressed_path.unlink()
                except Exception:
                    pass

    def download_data(self) -> List[Path]:
        """Download CHIRPS data for the specified date range"""
        dates = self.tools.generate_dates(self.start_date, self.end_date)
        download_paths = []
        chirps_config = next(iter(self.config['datasets'].values()))
        var_name = chirps_config['output_dir']
        naming_template = chirps_config.get('file_naming', "{variable}_{date}.tif.gz")

        for date in dates:
            year = date.split('-')[0]
            year_path = self.chirps_path / year
            year_path.mkdir(exist_ok=True)
            
            filename = naming_template.format(
                variable=var_name, 
                date=date.replace('-', '')
            )
            download_paths.append(year_path / filename)

        with ThreadPoolExecutor(max_workers=self.cores) as executor:
            urls = [self._build_download_url(date) for date in dates]
            executor.map(self._download_file, urls, download_paths)

        return [p.with_suffix('') for p in download_paths]

    def main(self):
        """Main processing pipeline"""
        try:
            return self.download_data()
        except Exception as e:
            error("Pipeline failed", component="main", error=str(e))
            raise