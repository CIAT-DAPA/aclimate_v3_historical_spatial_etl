import os
from datetime import datetime, timedelta
from tqdm import tqdm
import shutil
from .logging_manager import error, info, warning

class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        """
        Updates the progress bar for file downloads.
        
        Args:
            b (int): Number of blocks transferred
            bsize (int): Size of each block
            tsize (int): Total size of the file
        """
        try:
            if tsize is not None:
                self.total = tsize
            self.update(b * bsize - self.n)
        except Exception as e:
            error("Failed to update download progress",
                  component="download",
                  error=str(e))
            raise

class Tools():
    def get_date(self, month=None, current_date=datetime.now()):
        """
        Gets the first day of the specified or current month.
        
        Args:
            month (int, optional): Month number (1-12). Defaults to current month.
            current_date (datetime, optional): Reference date. Defaults to now.
            
        Returns:
            datetime: First day of the specified month
        """
        try:
            year = current_date.year
            
            if month is None:
                month = current_date.month
                info("Using current month",
                     component="date_utils",
                     current_month=month)
            
            first_day_of_month = datetime(year, month, 1)
            
            info("Generated first day of month",
                 component="date_utils",
                 year=year,
                 month=month,
                 result=first_day_of_month.strftime("%Y-%m-%d"))
            
            return first_day_of_month
        except Exception as e:
            error("Failed to get date",
                  component="date_utils",
                  month=month,
                  current_date=current_date,
                  error=str(e))
            raise

    def generate_dates(self, start_date, end_date):
        """
        Generates a list of dates between two dates (inclusive).
        Handles both monthly (YYYY-MM) and daily (YYYY-MM-DD) formats.

        Args:
            start_date (str): Start date in "YYYY-MM-DD" or "YYYY-MM" format
            end_date (str): End date in "YYYY-MM-DD" or "YYYY-MM" format

        Returns:
            list: List of dates as strings in "YYYY-MM-DD" format
        """
        try:
            # Parse start date
            if len(start_date.split('-')) == 2:  # YYYY-MM format
                start_date = datetime.strptime(start_date, "%Y-%m").replace(day=1)
            else:  # YYYY-MM-DD format
                start_date = datetime.strptime(start_date, "%Y-%m-%d")

            # Parse end date
            if len(end_date.split('-')) == 2:  # YYYY-MM format
                end_date = datetime.strptime(end_date, "%Y-%m")
                # Set to last day of month
                next_month = end_date.replace(day=28) + timedelta(days=4)
                end_date = next_month - timedelta(days=next_month.day)
            else:  # YYYY-MM-DD format
                end_date = datetime.strptime(end_date, "%Y-%m-%d")

            # Generate all dates in range
            dates = []
            current_date = start_date
            while current_date <= end_date:
                dates.append(current_date.strftime("%Y-%m-%d"))
                current_date += timedelta(days=1)

            return dates
        except Exception as e:
            error("Date generation failed", error=str(e))
            raise

    def create_dir(self, path):
        """
        Creates a directory if it doesn't exist.
        
        Args:
            path (str): Path to directory to create
        """
        try:
            if not os.path.exists(path):
                os.makedirs(path)
                info("Directory created",
                     component="file_utils",
                     path=path)
            else:
                info("Directory already exists",
                     component="file_utils",
                     path=path)
        except Exception as e:
            error("Failed to create directory",
                  component="file_utils",
                  path=path,
                  error=str(e))
            raise

    def copy_contents(self, src, dest):
        """
        Copies contents from source to destination directory.
        
        Args:
            src (str): Source directory path
            dest (str): Destination directory path
        """
        try:
            info("Starting directory copy",
                 component="file_utils",
                 source=src,
                 destination=dest)
            
            if not os.path.exists(dest):
                os.makedirs(dest)
                info("Created destination directory",
                     component="file_utils",
                     path=dest)
            
            items_copied = 0
            for item in os.listdir(src):
                s = os.path.join(src, item)
                d = os.path.join(dest, item)
                
                try:
                    if os.path.isdir(s):
                        shutil.copytree(s, d)
                    else:
                        shutil.copy2(s, d)
                    items_copied += 1
                    info("Copied item",
                         component="file_utils",
                         source=s,
                         destination=d)
                except Exception as e:
                    warning("Failed to copy item",
                            component="file_utils",
                            item=item,
                            error=str(e))
            
            info("Directory copy completed",
                 component="file_utils",
                 items_copied=items_copied,
                 total_items=len(os.listdir(src)))
        except Exception as e:
            error("Directory copy failed",
                  component="file_utils",
                  source=src,
                  destination=dest,
                  error=str(e))
            raise

    def has_file(self, directory):
        """
        Checks if a directory contains any files.
        
        Args:
            directory (str): Path to directory to check
            
        Returns:
            bool: True if directory contains files, False otherwise
        """
        try:
            for root, dirs, files in os.walk(directory):
                if files:
                    info("Directory contains files",
                         component="file_utils",
                         path=directory,
                         file_count=len(files))
                    return True
            
            info("Directory is empty",
                 component="file_utils",
                 path=directory)
            return False
        except Exception as e:
            error("Failed to check directory contents",
                  component="file_utils",
                  path=directory,
                  error=str(e))
            raise

    def validate_dates(self, start_date_str, end_date_str):
        """
        Validates that start date is before end date.
        
        Args:
            start_date_str (str): Start date in "YYYY-MM" format
            end_date_str (str): End date in "YYYY-MM" format
            
        Raises:
            ValueError: If start date is after end date
        """
        try:
            DATE_FORMAT = "%Y-%m"
            info("Validating date range",
                 component="validation",
                 start_date=start_date_str,
                 end_date=end_date_str)
            
            start_date = datetime.strptime(start_date_str, DATE_FORMAT)
            end_date = datetime.strptime(end_date_str, DATE_FORMAT)

            if start_date > end_date:
                error("Invalid date range - start date after end date",
                      component="validation",
                      start_date=start_date_str,
                      end_date=end_date_str)
                raise ValueError("The start date must be before the end date.")
            
            info("Date range validated successfully",
                 component="validation")
        except Exception as e:
            error("Date validation failed",
                  component="validation",
                  error=str(e))
            raise