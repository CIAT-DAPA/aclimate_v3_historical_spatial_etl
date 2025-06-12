import os
import shutil
from pathlib import Path
from aclimate_v3_spatial_importer import upload_image_mosaic

class GeoServerUploadPreparer:
    def __init__(self, source_data_path, upload_base_path):
        """
        Prepares TIFF files for GeoServer upload by organizing them in a temporary structure.
        
        Args:
            source_data_path (str): Path where original data is stored (structure: output/variable/year/tifs)
            upload_base_path (str): Base path where upload directory will be created
        """
        self.source_data_path = Path(source_data_path)
        self.upload_base_path = Path(upload_base_path)
        self.upload_dir = self.upload_base_path / "upload_geoserver"
        
        print(f"Initialized upload preparer with:")
        print(f"Source data path: {self.source_data_path}")
        print(f"Upload base path: {self.upload_base_path}")
        print(f"Final upload dir: {self.upload_dir}")
    
    def prepare_for_upload(self, variable):
        """
        Organizes all TIFF files into the upload directory (copies files).
        Works with both structures:
        1. With year subdirectories: variable/year/*.tif
        2. Flat structure: variable/*.tif
        
        Args:
            variable (str): Name of the variable to process (e.g., "precipitation")
            
        Returns:
            Path: Path to the prepared upload directory
            
        Raises:
            ValueError: If directories are not found
        """
        print(f"\nStarting preparation for variable: {variable}")
        
        # Create upload directory (and parent directories if needed)
        print(f"Preparing upload directory: {self.upload_dir}")
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        print("Upload directory ready")
        
        # Find source files
        variable_dir = self.source_data_path / variable
        if not variable_dir.exists():
            error_msg = f"Variable directory not found: {variable_dir}"
            print(f"ERROR: {error_msg}")
            raise ValueError(error_msg)
            
        print(f"Scanning for TIFF files in: {variable_dir}")
        
        # Determine if we have year subdirectories or flat structure
        tif_files = []
        has_year_subdirs = False
        
        # Check if first level contains directories (potential year directories)
        first_level_items = list(variable_dir.glob("*"))
        if all(item.is_dir() for item in first_level_items):
            print("Detected year subdirectory structure")
            has_year_subdirs = True
            year_count = 0
            tif_count = 0
            
            for year_dir in first_level_items:
                year_count += 1
                year_tifs = list(year_dir.glob("*.tif"))
                tif_count += len(year_tifs)
                tif_files.extend(year_tifs)
                print(f"Found {len(year_tifs)} TIFFs in {year_dir.name}")
                
            print(f"\nSummary (year subdirectories):")
            print(f"Total years processed: {year_count}")
            print(f"Total TIFF files found: {tif_count}")
        else:
            print("Detected flat directory structure (no year subdirectories)")
            tif_files = list(variable_dir.glob("*.tif"))
            print(f"Found {len(tif_files)} TIFF files directly in variable directory")
        
        if not tif_files:
            print("Warning: No TIFF files found for upload")
            return self.upload_dir
            
        # Copy files to upload directory (never move original files)
        print("\nCopying files to upload directory...")
        for tif in tif_files:
            dest = self.upload_dir / tif.name
            shutil.copy2(tif, dest)
            print(f"Copied: {tif.name} -> {dest}")
            
        print(f"\nSuccessfully prepared {len(tif_files)} files in upload directory")
        return self.upload_dir

    def upload_to_geoserver(self, workspace, store, date_format="yyyyMM"):
        """
        Uploads the prepared files to GeoServer.
        
        Args:
            workspace (str): GeoServer workspace name
            store (str): GeoServer store name
            date_format (str): Date format for time dimension
        """
        print("\n=== Starting GeoServer Upload ===")
        print(f"Uploading from: {self.upload_dir}")
        print(f"Workspace: {workspace}")
        print(f"Store: {store}")
        print(f"Date format: {date_format}")
        
        try:
            upload_image_mosaic(
                workspace=workspace,
                store=store,
                raster_dir=str(self.upload_dir),
                date_format=date_format
            )
            print("Upload completed successfully!")
        except Exception as e:
            print(f"Error during upload: {str(e)}")
            raise

    def clean_upload_dir(self):
        """Cleans the upload directory (commented out as requested)"""
        print("\n=== Clean Upload Directory ===")
        print(f"Would normally clean: {self.upload_dir}")
        # if self.upload_dir.exists():
        #     print("Removing all files from upload directory...")
        #     shutil.rmtree(self.upload_dir)
        #     self.upload_dir.mkdir()
        #     print("Upload directory cleaned")
        # else:
        #     print("Upload directory doesn't exist - nothing to clean")
        print("(Clean operation is currently commented out)")