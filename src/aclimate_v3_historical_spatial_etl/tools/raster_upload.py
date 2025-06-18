import os
import shutil
from pathlib import Path
from aclimate_v3_spatial_importer import upload_image_mosaic
from .logging_manager import error, info, warning

class GeoServerUploadPreparer:
    def __init__(self, source_data_path, upload_base_path):
        """
        Prepares TIFF files for GeoServer upload by organizing them in a temporary structure.
        
        Args:
            source_data_path (str): Path where original data is stored (structure: output/variable/year/tifs)
            upload_base_path (str): Base path where upload directory will be created
        """
        try:
            self.source_data_path = Path(source_data_path)
            self.upload_base_path = Path(upload_base_path)
            self.upload_dir = self.upload_base_path / "upload_geoserver"
            
            info("GeoServerUploadPreparer initialized",
                 component="initialization",
                 source_data_path=str(self.source_data_path),
                 upload_base_path=str(self.upload_base_path),
                 upload_dir=str(self.upload_dir))
                
        except Exception as e:
            error("Failed to initialize GeoServerUploadPreparer",
                  component="initialization",
                  error=str(e))
            raise
    
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
        try:
            info("Starting upload preparation",
                 component="preparation",
                 variable=variable)
            
            # Create upload directory
            info("Creating upload directory",
                 component="preparation",
                 upload_dir=str(self.upload_dir))
            self.upload_dir.mkdir(parents=True, exist_ok=True)
            
            # Find source files
            variable_dir = self.source_data_path / variable
            if not variable_dir.exists():
                error_msg = f"Variable directory not found: {variable_dir}"
                error("Variable directory not found",
                      component="preparation",
                      variable=variable,
                      path=str(variable_dir))
                raise ValueError(error_msg)
                
            info("Scanning for TIFF files",
                 component="preparation",
                 variable_dir=str(variable_dir))
            
            # Determine directory structure
            tif_files = []
            has_year_subdirs = False
            first_level_items = list(variable_dir.glob("*"))
            
            if all(item.is_dir() for item in first_level_items):
                info("Detected year subdirectory structure",
                     component="preparation")
                has_year_subdirs = True
                year_count = 0
                tif_count = 0
                
                for year_dir in first_level_items:
                    year_count += 1
                    year_tifs = list(year_dir.glob("*.tif"))
                    tif_count += len(year_tifs)
                    tif_files.extend(year_tifs)
                    info(f"Found TIFFs in year directory",
                         component="preparation",
                         year=year_dir.name,
                         file_count=len(year_tifs))
                
                info("Year subdirectory summary",
                     component="preparation",
                     years_processed=year_count,
                     total_files=tif_count)
            else:
                info("Detected flat directory structure",
                     component="preparation")
                tif_files = list(variable_dir.glob("*.tif"))
                info("Found TIFF files in variable directory",
                     component="preparation",
                     file_count=len(tif_files))
            
            if not tif_files:
                warning("No TIFF files found for upload",
                        component="preparation",
                        variable=variable)
                return self.upload_dir
                
            # Copy files to upload directory
            info("Copying files to upload directory",
                 component="preparation",
                 file_count=len(tif_files))
            
            copied_count = 0
            for tif in tif_files:
                try:
                    dest = self.upload_dir / tif.name
                    shutil.copy2(tif, dest)
                    copied_count += 1
                    info("Copied file",
                         component="preparation",
                         source=str(tif),
                         destination=str(dest))
                except Exception as e:
                    warning("Failed to copy file",
                            component="preparation",
                            file=str(tif),
                            error=str(e))
            
            info("Upload preparation completed",
                 component="preparation",
                 variable=variable,
                 files_copied=copied_count,
                 upload_dir=str(self.upload_dir))
                
            return self.upload_dir
            
        except Exception as e:
            error("Upload preparation failed",
                  component="preparation",
                  variable=variable,
                  error=str(e))
            raise

    def upload_to_geoserver(self, workspace, store, date_format="yyyyMM"):
        """
        Uploads the prepared files to GeoServer.
        
        Args:
            workspace (str): GeoServer workspace name
            store (str): GeoServer store name
            date_format (str): Date format for time dimension
        """
        try:
            info("Starting GeoServer upload",
                 component="upload",
                 workspace=workspace,
                 store=store,
                 date_format=date_format,
                 source_dir=str(self.upload_dir))
            
            upload_image_mosaic(
                workspace=workspace,
                store=store,
                raster_dir=str(self.upload_dir),
                date_format=date_format
            )
            
            info("GeoServer upload completed successfully",
                 component="upload",
                 workspace=workspace,
                 store=store)
                
        except Exception as e:
            error("GeoServer upload failed",
                  component="upload",
                  workspace=workspace,
                  store=store,
                  error=str(e))
            raise

    def clean_upload_dir(self):
        """Cleans the upload directory (commented out as requested)"""
        try:
            info("Starting upload directory cleanup",
                 component="cleanup",
                 upload_dir=str(self.upload_dir))
            
            if self.upload_dir.exists():
                info("Removing files from upload directory",
                     component="cleanup")
                shutil.rmtree(self.upload_dir)
                self.upload_dir.mkdir()
                info("Upload directory cleaned",
                     component="cleanup")
            else:
                info("Upload directory doesn't exist - nothing to clean",
                     component="cleanup")
            
            info("Clean operation is currently commented out",
                 component="cleanup")
                
        except Exception as e:
            error("Failed to clean upload directory",
                  component="cleanup",
                  error=str(e))
            raise