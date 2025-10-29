import os
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import calculate_default_transform, reproject
from pathlib import Path
from typing import Union, Optional
from .logging_manager import info, error, warning


class RasterResampler:
    """
    Utility class for resampling raster files to a specific resolution.
    Designed specifically for ERA5 data processing to match CHIRPS resolution.
    """
    
    def __init__(self, target_resolution: Optional[float] = None):
        """
        Initialize the RasterResampler.
        
        Args:
            target_resolution: Target resolution in degrees. If None, will try to get from environment variable.
        """
        self.target_resolution = target_resolution or self._get_resolution_from_env()
        
        if self.target_resolution is None:
            error("No target resolution specified and RASTER_TARGET_RESOLUTION environment variable not set",
                  component="resampler")
            raise ValueError("Target resolution must be specified either as parameter or environment variable")
        
        info(f"RasterResampler initialized with target resolution: {self.target_resolution} degrees",
             component="resampler",
             target_resolution=self.target_resolution)
    
    def _get_resolution_from_env(self) -> Optional[float]:
        """Get target resolution from environment variable."""
        try:
            env_resolution = os.getenv('RASTER_TARGET_RESOLUTION')
            if env_resolution:
                resolution = float(env_resolution)
                info(f"Target resolution loaded from environment variable: {resolution}",
                     component="resampler",
                     source="environment")
                return resolution
            return None
        except ValueError as e:
            error("Invalid RASTER_TARGET_RESOLUTION environment variable format",
                  component="resampler",
                  env_value=env_resolution,
                  error=str(e))
            return None
    
    def resample_raster(self, input_path: Union[str, Path], output_path: Union[str, Path],
                       resampling_method: Resampling = Resampling.bilinear,
                       compress: str = 'lzw', dtype: str = 'float32') -> bool:
        """
        Resample a single raster file to the target resolution.
        
        Args:
            input_path: Path to input raster file
            output_path: Path for output resampled raster file
            resampling_method: Resampling algorithm to use (default: bilinear)
            compress: Compression method for output file
            dtype: Data type for output file
            
        Returns:
            bool: True if successful, False otherwise
        """
        input_path = Path(input_path)
        output_path = Path(output_path)
        
        if not input_path.exists():
            error("Input raster file does not exist",
                  component="resampler",
                  input_path=str(input_path))
            return False
        
        try:
            info(f"Starting raster resampling from {input_path.name}",
                 component="resampler",
                 input_file=str(input_path),
                 output_file=str(output_path),
                 target_resolution=self.target_resolution,
                 resampling_method=resampling_method.name)
            
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with rasterio.open(input_path) as src:
                # Calculate new transform and dimensions
                transform, width, height = calculate_default_transform(
                    src.crs, src.crs, src.width, src.height, *src.bounds, 
                    resolution=self.target_resolution
                )
                
                # Update profile for output raster
                profile = src.profile.copy()
                profile.update({
                    'transform': transform,
                    'width': width,
                    'height': height,
                    'compress': compress,
                    'dtype': dtype
                })
                
                info(f"Resampling parameters calculated",
                     component="resampler",
                     original_size=(src.width, src.height),
                     new_size=(width, height),
                     original_resolution=self._calculate_resolution(src),
                     target_resolution=self.target_resolution)
                
                # Perform resampling
                with rasterio.open(output_path, 'w', **profile) as dst:
                    reproject(
                        source=rasterio.band(src, 1),
                        destination=rasterio.band(dst, 1),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=src.crs,
                        resampling=resampling_method
                    )
            
            # Verify output file was created
            if output_path.exists():
                file_size = output_path.stat().st_size / (1024 * 1024)  # MB
                info(f"Raster resampling completed successfully",
                     component="resampler",
                     output_file=str(output_path),
                     file_size_mb=f"{file_size:.2f}")
                return True
            else:
                error("Output file was not created",
                      component="resampler",
                      output_path=str(output_path))
                return False
                
        except Exception as e:
            error("Raster resampling failed",
                  component="resampler",
                  input_path=str(input_path),
                  output_path=str(output_path),
                  error=str(e))
            return False
    
    def resample_directory(self, input_dir: Union[str, Path], output_dir: Union[str, Path],
                          pattern: str = "*.tif", overwrite: bool = False,
                          resampling_method: Resampling = Resampling.bilinear) -> dict:
        """
        Resample all raster files in a directory.
        
        Args:
            input_dir: Directory containing input raster files
            output_dir: Directory for output resampled files
            pattern: File pattern to match (default: "*.tif")
            overwrite: Whether to overwrite existing files
            resampling_method: Resampling algorithm to use
            
        Returns:
            dict: Summary with counts of successful, failed, and skipped files
        """
        input_dir = Path(input_dir)
        output_dir = Path(output_dir)
        
        if not input_dir.exists():
            error("Input directory does not exist",
                  component="resampler",
                  input_dir=str(input_dir))
            return {"successful": 0, "failed": 0, "skipped": 0}
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        info(f"Starting batch resampling",
             component="resampler",
             input_dir=str(input_dir),
             output_dir=str(output_dir),
             pattern=pattern)
        
        summary = {"successful": 0, "failed": 0, "skipped": 0}
        
        # Find all matching files
        raster_files = list(input_dir.glob(pattern))
        
        if not raster_files:
            warning("No raster files found matching pattern",
                   component="resampler",
                   input_dir=str(input_dir),
                   pattern=pattern)
            return summary
        
        info(f"Found {len(raster_files)} files to process",
             component="resampler",
             file_count=len(raster_files))
        
        for raster_file in raster_files:
            output_file = output_dir / raster_file.name
            
            # Check if output file already exists
            if output_file.exists() and not overwrite:
                info(f"Skipping existing file: {raster_file.name}",
                     component="resampler",
                     file=str(raster_file))
                summary["skipped"] += 1
                continue
            
            # Perform resampling
            if self.resample_raster(raster_file, output_file, resampling_method):
                summary["successful"] += 1
            else:
                summary["failed"] += 1
        
        info(f"Batch resampling completed",
             component="resampler",
             **summary)
        
        return summary
    
    def resample_raster_inplace(self, raster_path: Union[str, Path],
                               backup: bool = True,
                               resampling_method: Resampling = Resampling.bilinear) -> bool:
        """
        Resample a raster file in place (overwrites original).
        
        Args:
            raster_path: Path to the raster file to resample
            backup: Whether to create a backup of the original file
            resampling_method: Resampling algorithm to use
            
        Returns:
            bool: True if successful, False otherwise
        """
        raster_path = Path(raster_path)
        
        if not raster_path.exists():
            error("Raster file does not exist",
                  component="resampler",
                  raster_path=str(raster_path))
            return False
        
        try:
            # Create backup if requested
            backup_path = None
            if backup:
                backup_path = raster_path.with_suffix(f"{raster_path.suffix}.backup")
                info(f"Creating backup of original file",
                     component="resampler",
                     original=str(raster_path),
                     backup=str(backup_path))
                import shutil
                shutil.copy2(raster_path, backup_path)
            
            # Create temporary output file
            temp_path = raster_path.with_suffix(f"{raster_path.suffix}.temp")
            
            # Perform resampling
            success = self.resample_raster(raster_path, temp_path, resampling_method)
            
            if success:
                # Replace original with resampled version
                raster_path.unlink()  # Delete original
                temp_path.rename(raster_path)  # Rename temp to original
                
                # Clean up backup if everything went well and not requested to keep
                if backup_path and backup_path.exists():
                    info(f"Backup available at: {backup_path}",
                         component="resampler",
                         backup_path=str(backup_path))
                
                info(f"In-place resampling completed successfully",
                     component="resampler",
                     file=str(raster_path))
                return True
            else:
                # Clean up temp file on failure
                if temp_path.exists():
                    temp_path.unlink()
                
                # Restore from backup if it exists
                if backup_path and backup_path.exists():
                    backup_path.rename(raster_path)
                    info(f"Original file restored from backup",
                         component="resampler",
                         file=str(raster_path))
                
                return False
                
        except Exception as e:
            error("In-place resampling failed",
                  component="resampler",
                  raster_path=str(raster_path),
                  error=str(e))
            return False
    
    def _calculate_resolution(self, src) -> float:
        """Calculate the resolution of a raster dataset."""
        try:
            # Get the pixel size from the transform
            pixel_size_x = abs(src.transform[0])
            pixel_size_y = abs(src.transform[4])
            # Return the average (assuming square pixels)
            return (pixel_size_x + pixel_size_y) / 2
        except Exception:
            return 0.0
    
    def get_raster_info(self, raster_path: Union[str, Path]) -> dict:
        """
        Get information about a raster file.
        
        Args:
            raster_path: Path to the raster file
            
        Returns:
            dict: Raster information including resolution, size, bounds, etc.
        """
        raster_path = Path(raster_path)
        
        if not raster_path.exists():
            error("Raster file does not exist",
                  component="resampler",
                  raster_path=str(raster_path))
            return {}
        
        try:
            with rasterio.open(raster_path) as src:
                resolution = self._calculate_resolution(src)
                
                info_dict = {
                    'file_path': str(raster_path),
                    'width': src.width,
                    'height': src.height,
                    'resolution': resolution,
                    'crs': str(src.crs),
                    'bounds': src.bounds,
                    'dtype': str(src.dtypes[0]),
                    'nodata': src.nodata,
                    'band_count': src.count
                }
                
                info(f"Raster information retrieved",
                     component="resampler",
                     file=raster_path.name,
                     resolution=resolution,
                     size=(src.width, src.height))
                
                return info_dict
                
        except Exception as e:
            error("Failed to get raster information",
                  component="resampler",
                  raster_path=str(raster_path),
                  error=str(e))
            return {}