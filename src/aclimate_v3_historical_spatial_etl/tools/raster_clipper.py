import os
from pathlib import Path
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from aclimate_v3_cut_spatial_data import get_clipper, GeoServerBasicAuth
import re
import threading
from .logging_manager import info, warning, error

class RasterClipper:
    def __init__(self, 
                 country: str,
                 downloader_configs: Dict[str, Dict],
                 naming_config: Dict,
                 clipping_config: Dict):
        """
        Clips raster files to country boundaries using GeoServer.
        
        Args:
            country: Target country (e.g., "COLOMBIA")
            downloader_configs: Dictionary with {'downloader_name': config_dict}
            naming_config: Dictionary with naming configuration
            clipping_config: Dictionary with clipping configuration
        """
        try:
            self.country = country.upper()
            self.downloader_configs = downloader_configs
            self.naming_config = naming_config  # Recibe dict directamente
            self.clipping_config = clipping_config  # Recibe dict directamente
            
            # Configure parallel processing
            self.max_workers = int(os.getenv('MAX_PARALLEL_DOWNLOADS', 4))
            
            # Validación del país
            if self.country not in self.clipping_config['countries']:
                error("Country not found in configuration",
                      component="validation",
                      country=country,
                      available_countries=list(self.clipping_config['countries'].keys()))
                raise ValueError(f"Country '{country}' not found in configuration")
            
            self.country_config = self.clipping_config['countries'][self.country]
            self.conn = GeoServerBasicAuth()  # Keep for non-parallel operations
            
            # Thread-local storage for per-thread connections
            self.thread_local = threading.local()
            
            info("RasterClipper initialized successfully",
                 component="initialization",
                 country=country,
                 max_workers=self.max_workers)
                
        except Exception as e:
            error("Failed to initialize RasterClipper",
                  component="initialization",
                  error=str(e))
            raise

    def _get_thread_connection(self):
        """Get or create a GeoServer connection for the current thread"""
        if not hasattr(self.thread_local, 'connection'):
            self.thread_local.connection = GeoServerBasicAuth()
        return self.thread_local.connection

    # Eliminado el método _load_config ya que no es necesario

    def _get_variable_mapping(self, config: Dict) -> Dict:
        """Get variable mapping from a config dictionary"""
        try:
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
                 variables=list(mapping.keys()))
            return mapping
            
        except Exception as e:
            error("Failed to extract variable mapping",
                  component="config",
                  error=str(e))
            raise

    # Todos los demás métodos se mantienen EXACTAMENTE igual
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

    def _process_raster_task(self, task_info: Dict) -> Dict:
        """
        Process a single raster file - helper method for parallel processing.
        
        Args:
            task_info: Dictionary with task information including raster_file, var_name, output_dir
            
        Returns:
            dict: Result with status and file information
        """
        raster_file = task_info['raster_file']
        var_name = task_info['var_name']
        output_dir = task_info['output_dir']
        
        result = {
            'raster_file': str(raster_file),
            'var_name': var_name,
            'status': 'failed',
            'reason': '',
            'output_file': None
        }
        
        try:
            success = self._process_raster(raster_file, var_name, output_dir)
            if success:
                result['status'] = 'successful'
                result['reason'] = 'Raster processing completed successfully'
            else:
                result['status'] = 'skipped'
                result['reason'] = 'File already exists or invalid date format'
                
        except Exception as e:
            result['status'] = 'failed'
            result['reason'] = f'Exception occurred: {str(e)}'
            error("Raster processing task failed",
                  component="processing",
                  raster_file=str(raster_file),
                  var_name=var_name,
                  error=str(e))
        
        return result

    def process_all(self, base_download_path: Path, base_processed_path: Path):
        """Process all downloaded data"""
        try:
            info("Starting raster clipping process",
                 component="processing",
                 base_download_path=str(base_download_path),
                 base_processed_path=str(base_processed_path))
            
            for downloader_name, config in self.downloader_configs.items():
                info(f"Processing data from downloader",
                     component="processing",
                     downloader_name=downloader_name)
                
                var_mapping = self._get_variable_mapping(config)

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
        """Process all files for a given variable with parallel processing"""
        try:
            info("Processing variable with parallel processing",
                 component="processing",
                 variable=var_name,
                 input_path=str(input_path),
                 output_path=str(output_path),
                 max_workers=self.max_workers)
            
            # Collect all raster files that need processing
            processing_tasks = []
            
            for year_dir in input_path.glob("*"):
                if not year_dir.is_dir():
                    continue
                    
                output_year_path = output_path / year_dir.name
                output_year_path.mkdir(parents=True, exist_ok=True)
                
                for raster_file in year_dir.glob("*.tif"):
                    processing_tasks.append({
                        'raster_file': raster_file,
                        'var_name': var_name,
                        'output_dir': output_year_path
                    })
            
            if not processing_tasks:
                info("No raster files found to process",
                     component="processing",
                     variable=var_name)
                return
            
            info(f"Found {len(processing_tasks)} raster files to process",
                 component="processing",
                 variable=var_name,
                 file_count=len(processing_tasks))
            
            # Process files in parallel
            files_processed = 0
            files_skipped = 0
            errors = 0
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_task = {
                    executor.submit(self._process_raster_task, task): task
                    for task in processing_tasks
                }
                
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        result = future.result()
                        
                        if result['status'] == 'successful':
                            files_processed += 1
                            info(f"Raster processed successfully: {Path(result['raster_file']).name}",
                                 component="processing",
                                 file=result['raster_file'],
                                 var_name=result['var_name'])
                        elif result['status'] == 'skipped':
                            files_skipped += 1
                            info(f"Raster skipped: {Path(result['raster_file']).name}",
                                 component="processing",
                                 file=result['raster_file'],
                                 reason=result['reason'])
                        else:
                            errors += 1
                            error(f"Raster processing failed: {Path(result['raster_file']).name}",
                                  component="processing",
                                  file=result['raster_file'],
                                  reason=result['reason'])
                                  
                    except Exception as e:
                        errors += 1
                        error("Processing task execution failed",
                              component="processing",
                              raster_file=str(task['raster_file']),
                              var_name=task['var_name'],
                              error=str(e))
            
            info(f"Variable processing completed {var_name}: {files_processed} files processed, {files_skipped} files skipped, {errors} errors",
                 component="processing",
                 variable=var_name,
                 files_processed=files_processed,
                 files_skipped=files_skipped,
                 errors=errors,
                 total_files=len(processing_tasks),
                 success_rate=f"{(files_processed/len(processing_tasks))*100:.1f}%" if len(processing_tasks) > 0 else "0%")
                
        except Exception as e:
            error(f"Failed to process variable {var_name}",
                  component="processing",
                  variable=var_name,
                  error=str(e))
            raise
    
    def process_variables_parallel(self, variable_tasks: list) -> Dict:
        """
        Process multiple variables in parallel.
        
        Args:
            variable_tasks: List of dictionaries with keys: var_name, input_path, output_path
            
        Returns:
            dict: Summary with processing statistics
        """
        if not variable_tasks:
            warning("No variable tasks provided for processing",
                   component="processing")
            return {"successful": 0, "failed": 0, "total_files": 0}
        
        info(f"Starting parallel processing of {len(variable_tasks)} variables",
             component="processing",
             variable_count=len(variable_tasks),
             max_workers=self.max_workers)
        
        summary = {"successful": 0, "failed": 0, "total_files": 0}
        
        # Execute variable processing in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_task = {
                executor.submit(self._process_variable_wrapper, task): task
                for task in variable_tasks
            }
            
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    summary["successful"] += result.get("files_processed", 0)
                    summary["failed"] += result.get("errors", 0)
                    summary["total_files"] += result.get("total_files", 0)
                    
                except Exception as e:
                    summary["failed"] += 1
                    error("Variable processing task failed",
                          component="processing",
                          var_name=task.get('var_name'),
                          error=str(e))
        
        info(f"Parallel variable processing completed",
             component="processing",
             variables_processed=len(variable_tasks),
             **summary,
             success_rate=f"{(summary['successful']/summary['total_files'])*100:.1f}%" if summary['total_files'] > 0 else "0%")
        
        return summary
    
    def _process_variable_wrapper(self, task: Dict) -> Dict:
        """Wrapper method for parallel variable processing"""
        var_name = task['var_name']
        input_path = task['input_path']
        output_path = task['output_path']
        
        try:
            self._process_variable(var_name, input_path, output_path)
            # Count files for statistics
            total_files = sum(1 for _ in input_path.glob("**/*.tif"))
            return {
                "var_name": var_name,
                "status": "success",
                "files_processed": 1,  # This would need to be tracked in _process_variable
                "total_files": total_files,
                "errors": 0
            }
        except Exception as e:
            error(f"Failed to process variable {var_name}",
                  component="processing",
                  var_name=var_name,
                  error=str(e))
            return {
                "var_name": var_name,
                "status": "failed",
                "files_processed": 0,
                "total_files": 0,
                "errors": 1
            }

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

            info(f"Processing raster file {str(input_file)} to {str(output_file)}",
                 component="processing",
                 input_file=str(input_file),
                 output_file=str(output_file))
            
            # Get thread-specific connection for thread safety
            thread_conn = self._get_thread_connection()
            clipper = get_clipper(str(input_file), 'geoserver')
            clipper.connection = thread_conn
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

    def clean_processed_data_parallel(self, base_processed_path: Path, confirm: bool = False):
        """
        Deletes all processed raster files with parallel processing while maintaining directory structure.
        
        Args:
            base_processed_path: Base path where processed data is stored
            confirm: If True, asks for confirmation before deletion (safety measure)
        """
        try:
            info("Starting parallel processed data cleanup",
                 component="cleanup",
                 base_processed_path=str(base_processed_path),
                 confirm_required=confirm,
                 max_workers=self.max_workers)
            
            if not base_processed_path.exists():
                warning("Processed data path does not exist",
                        component="cleanup",
                        path=str(base_processed_path))
                return
                
            # Collect all raster files
            all_raster_files = list(base_processed_path.glob('**/*.tif'))
            total_files = len(all_raster_files)
            
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
            
            info(f"Deleting {total_files} files in parallel",
                 component="cleanup",
                 total_files=total_files)
            
            deleted_count = 0
            errors = 0
            
            # Process deletions in parallel
            def delete_file(file_path):
                try:
                    file_path.unlink()
                    return True
                except Exception as e:
                    error("Failed to delete raster file",
                          component="cleanup",
                          file=str(file_path),
                          error=str(e))
                    return False
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_file = {
                    executor.submit(delete_file, raster_file): raster_file
                    for raster_file in all_raster_files
                }
                
                for future in as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        success = future.result()
                        if success:
                            deleted_count += 1
                        else:
                            errors += 1
                    except Exception as e:
                        errors += 1
                        error("File deletion task failed",
                              component="cleanup",
                              file=str(file_path),
                              error=str(e))
            
            info("Parallel cleanup completed",
                 component="cleanup",
                 files_deleted=deleted_count,
                 total_files=total_files,
                 errors=errors,
                 success_rate=f"{(deleted_count/total_files)*100:.1f}%" if total_files > 0 else "0%")
                
        except Exception as e:
            error("Parallel cleanup process failed",
                  component="cleanup",
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