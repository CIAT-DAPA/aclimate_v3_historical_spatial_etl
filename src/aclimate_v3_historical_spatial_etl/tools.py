import os
from datetime import datetime, timedelta
from tqdm import tqdm
import shutil

class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)

class Tools():

  def get_date(self, month=None, current_date=datetime.now()):
    # Extract the year and month from the current date passed as a parameter
    year = current_date.year
    
    # If no month is provided, use the month from the current date
    if month is None:
        month = current_date.month
    
    # Create the date with the first day of the given or current month
    first_day_of_month = datetime(year, month, 1)
    
    # Format the date to return it as a string (optional)
    return first_day_of_month
  
  def generate_dates(self, start_date, end_date):
    # Convert the string dates to datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m")
    end_date = datetime.strptime(end_date, "%Y-%m")
    
    # Adjust the end date to the last day of the final month
    last_day = (end_date.replace(month=end_date.month % 12 + 1, day=1) - timedelta(days=1)).day
    end_date = end_date.replace(day=last_day)
    
    # Generate the list of dates day by day
    dates = []
    current_date = start_date.replace(day=1)
    
    while current_date <= end_date:
        dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    
    return dates


  def create_dir(self, path):
    if not os.path.exists(path):
      os.makedirs(path)

  def copy_contents(self, src, dest):
    if not os.path.exists(dest):
        os.makedirs(dest)


    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dest, item)
        shutil.copytree(s, d)
  
  def has_file(self, directory):
    for root, dirs, files in os.walk(directory):
        if files:
            return True
    return False
  
  def validate_dates(self, start_date_str, end_date_str):

    DATE_FORMAT = "%Y-%m"
    start_date = datetime.strptime(start_date_str, DATE_FORMAT)
    end_date = datetime.strptime(end_date_str, DATE_FORMAT)

    if start_date > end_date:
        raise ValueError("The start date must be greater than the end date.")