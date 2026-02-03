"""
Utility functions for cleanup operations and resource management.
"""
import os
import gc
import sys
import time
import shutil
import weakref
from pathlib import Path
from .logging_manager import error, warning, info


def force_cleanup_resources():
    """Force cleanup of resources that might be holding file handles"""
    try:
        # Force multiple rounds of garbage collection
        for _ in range(5):
            gc.collect()
            time.sleep(0.2)
        
        # Additional cleanup for Windows
        if os.name == 'nt':  # Windows
            # Force finalize any pending objects
            # Clear weak references that might be holding file handles
            weakref.getweakrefs(object())
        
        # Longer delay to allow OS to release file handles
        time.sleep(3.0)
        
        info("Forced resource cleanup completed", component="cleanup")
    except Exception as e:
        warning("Error during forced resource cleanup",
               component="cleanup", 
               error=str(e))


def safe_remove_file(file_path: Path, max_retries: int = 5) -> bool:
    """
    Safely remove a file with enhanced retry logic for Windows file locks.
    
    Returns:
        True if file was successfully removed, False otherwise
    """
    for attempt in range(max_retries):
        try:
            # Multiple attempts to change file permissions on Windows
            if os.name == 'nt':  # Windows
                for perm_attempt in range(3):
                    try:
                        # Try different permission combinations
                        if perm_attempt == 0:
                            os.chmod(file_path, 0o777)
                        elif perm_attempt == 1:
                            os.chmod(file_path, 0o666)
                        else:
                            os.chmod(file_path, 0o644)
                        break
                    except (OSError, PermissionError):
                        time.sleep(0.1)
                        continue
            
            # Force garbage collection before removal attempt
            gc.collect()
            time.sleep(0.1)
            
            # Try to remove the file
            file_path.unlink()
            return True
            
        except (OSError, PermissionError) as e:
            if attempt < max_retries - 1:
                if any(error_text in str(e).lower() for error_text in 
                       ["being used by another process", "winError 32", "access is denied", "sharing violation"]):
                    
                    # Exponential backoff: 1s, 2s, 4s, 8s
                    wait_time = 2 ** attempt
                    warning(f"File locked, retrying in {wait_time} seconds (attempt {attempt + 1}/{max_retries})",
                           component="cleanup",
                           file=str(file_path))
                    
                    # Multiple rounds of cleanup during wait
                    for _ in range(wait_time):
                        gc.collect()
                        time.sleep(1)
                else:
                    # Different error, don't retry
                    error(f"Non-recoverable file error: {str(e)}",
                          component="cleanup",
                          file=str(file_path))
                    break
            else:
                # Final attempt failed
                error(f"Could not remove file after {max_retries} attempts: {str(e)}",
                      component="cleanup",
                      file=str(file_path))
                return False
    
    return False


def clean_directory(path: Path, force: bool = False, max_retries: int = 3, retry_delay: int = 1):
    """Clean directory contents with safety checks and retry logic for Windows file locks."""
    if not path.exists():
        warning("Directory does not exist - skipping cleanup",
               component="cleanup",
               path=str(path))
        return
    
    if not force:
        # Check if running in interactive mode
        if sys.stdin.isatty():
            response = input(f"Are you sure you want to clean {path}? [y/N]: ")
            if response.lower() != 'y':
                info("Cleanup cancelled by user",
                     component="cleanup",
                     path=str(path))
                return
        else:
            warning("Non-interactive mode detected - skipping cleanup confirmation",
                   component="cleanup",
                   path=str(path))
            return
    
    # Force garbage collection to release any open file handles
    gc.collect()
    
    retry_count = 0
    items_deleted = 0
    failed_items = []
    
    while retry_count <= max_retries:
        try:
            # Convert to list to avoid iterator issues during deletion
            items_to_delete = list(path.glob("*"))
            failed_items_this_round = []
            
            for item in items_to_delete:
                try:
                    if item.is_file():
                        # Use safe removal for files
                        if safe_remove_file(item):
                            items_deleted += 1
                        else:
                            failed_items_this_round.append(str(item))
                    elif item.is_dir():
                        # For directories, use shutil.rmtree with error handling
                        def handle_remove_readonly(func, path, exc):
                            if os.name == 'nt':  # Windows
                                try:
                                    os.chmod(path, 0o777)
                                    func(path)
                                except (OSError, PermissionError):
                                    pass
                        
                        shutil.rmtree(item, onerror=handle_remove_readonly)
                        items_deleted += 1
                        
                except (OSError, PermissionError) as e:
                    if "being used by another process" in str(e) or "WinError 32" in str(e):
                        failed_items_this_round.append(str(item))
                        warning(f"File is being used by another process, will retry",
                               component="cleanup",
                               file=str(item),
                               retry_count=retry_count)
                    else:
                        error(f"Failed to delete item: {str(e)}",
                              component="cleanup",
                              item=str(item),
                              error=str(e))
                        failed_items_this_round.append(str(item))
            
            # If no failed items, we're done
            if not failed_items_this_round:
                info("Directory cleanup completed",
                     component="cleanup",
                     path=str(path),
                     items_deleted=items_deleted,
                     retries_used=retry_count)
                return
            
            # Store failed items for next retry
            failed_items = failed_items_this_round
            retry_count += 1
            
            if retry_count <= max_retries:
                info(f"Retrying cleanup in {retry_delay} seconds",
                     component="cleanup",
                     path=str(path),
                     failed_items_count=len(failed_items),
                     retry_count=retry_count)
                time.sleep(retry_delay)
                # Force garbage collection again before retry
                gc.collect()
                
        except Exception as e:
            error(f"Unexpected error during cleanup: {str(e)}",
                  component="cleanup",
                  path=str(path),
                  error=str(e))
            retry_count += 1
            if retry_count <= max_retries:
                time.sleep(retry_delay)
                gc.collect()
    
    # If we get here, some items couldn't be deleted
    if failed_items:
        warning(f"Could not delete {len(failed_items)} items after {max_retries} retries",
               component="cleanup",
               path=str(path),
               failed_items=failed_items[:5],  # Show first 5 failed items
               items_deleted=items_deleted)
        # Don't raise an error, just log the warning and continue
    else:
        info("Directory cleanup completed",
             component="cleanup",
             path=str(path),
             items_deleted=items_deleted,
             retries_used=retry_count)