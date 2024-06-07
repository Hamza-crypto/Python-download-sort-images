import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import requests
import csv
from urllib.parse import urlparse
import shutil
from datetime import datetime

# Base directory of the script file
BASE_DIR = Path(__file__).resolve().parent
# Directory to store downloaded files
DOWNLOAD_DIR = BASE_DIR / "downloads"
ERROR_DIR = DOWNLOAD_DIR / "errors"
ERROR_LOGS = DOWNLOAD_DIR / "errors"
error_folders = []
error_messages = []
error_urls = {}
# Log Directory
LOG_DIR = BASE_DIR / "logs"
# Create log directory if it doesn't exist
LOG_DIR.mkdir(parents=True, exist_ok=True)
# Default search path
SEARCH_PATH = r'C:\Users\Home\j m Dropbox\j m\rppgraphs eBay'
# BP search path
BP_SEARCH_PATH = r'E:\Installed\laragon\www\python_img_downloader_with_formatting\my_search_folder' 

# Configure logging
logger = logging.getLogger("FF")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Create a file handler and set its level to DEBUG
file_handler = RotatingFileHandler(LOG_DIR / 'app.log', maxBytes=1024*1024*10, backupCount=5)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

# Create a console handler and set its level to INFO
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

class FileFinder:
    CHUNK_SIZE = 1024  # Size of each chunk when downloading

    def __init__(self, search_path):
        """
        Initialize the FileFinder object with a search path.
        Load inventory and order data from CSV files.
        """
        self.search_path = Path(search_path).resolve()
        self.bp_search_path = Path(BP_SEARCH_PATH).resolve() # BP Search Path
        self._inventories = self.read_csv_file(BASE_DIR / "inventory.csv")
        self._orders = self.read_csv_file(BASE_DIR / "order.csv")

    def read_csv_file(self, file_path):
        """
        Generator function to read a CSV file and yield each row.
        """
        with open(file_path, "r", encoding="ISO-8859-1", newline="") as f:
            csv_reader = csv.reader(f, quotechar='"', delimiter=",")
            for row in csv_reader:
                yield row

    def get_inventories(self):
        """
        Get the loaded inventory data.
        """
        return self._inventories

    def get_orders(self):
        """
        Get the loaded order data.
        """
        return self._orders

    # Function to get the last run datetime
    def get_lastrun(self):
        try:
            with open(BASE_DIR / "lastrun","r",encoding="utf-8",newline="") as f:
                return datetime.strptime(f.read(), "%d/%m/%Y,%I:%M %p")
        except Exception as e:
            return None

    # Function to set the last run datetime
    def set_lastrun(self,dt):
        with open(BASE_DIR / "lastrun","w",encoding="utf-8",newline="") as f:
            f.write(dt)

    def find_file(self, file_name, bp=False):
        """
        Search for a file by name in the specified search path and its subdirectories.
        """
        search_path = self.bp_search_path if bp else self.search_path
        if file_name:
            for path in search_path.rglob('*'):
                if file_name in path.name:
                    return path
        return None

    def get_headers(self):
        """
        Get the default headers for HTTP requests.
        """
        return {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }

    def download_img(self, dest, URL, sc=False):
        
        """
        Download an image from the specified URL and save it to the destination file path.
        """
        filename = urlparse(URL).path.split("/")[-1]
        if sc:
            filename = self.rename_file_for_sc(filename)
            
        filepath = dest / filename
        try:
            with requests.get(URL, headers=self.get_headers(), stream=True) as r:
                if r.status_code != 200:
                    logger.error(f"Failed to download image from {URL}")
                    return False
                with open(filepath, 'wb') as file:
                    for chunk in r.iter_content(chunk_size=self.CHUNK_SIZE):
                        if chunk:
                            file.write(chunk)
        except Exception as e:
            logger.error(f"Failed to download image from {URL}")
            error_folders.append(dest)
            error_urls[URL] = dest
            return False
        return True
    
    def error_folders(self):
        """
        Iterate through subdirectories in the downloads folder and move any empty folders to the error folder
        """
        for sub_dir in DOWNLOAD_DIR.iterdir():
            if sub_dir.is_dir() and sub_dir != ERROR_DIR:
                for folder in sub_dir.iterdir():
                    if folder.is_dir() and not any(folder.iterdir()):
                        self.move_folders(folder)

        for folder in error_folders:
            try:
                self.move_folders(folder)
            except Exception as e:
                continue

        error_string = ""
        for key, value in error_urls.items():
            error_string += f"URL: {key}, Folder: {value}\n"

        ERROR_DIR.mkdir(parents=True, exist_ok=True)


        with open(ERROR_LOGS / "errors.txt", "wb") as f:
            f.write(error_string.encode("utf-8"))

        with open(ERROR_LOGS / "errors.txt", "wb") as f:
            for msg in error_messages:
                f.write(msg.encode("utf-8"))
                f.write(b"\n")

        print(error_messages)
        
                
    def move_folders(self, dest):
        """
        MOVE THE DEST FOLDER INTO THE ERROR FOLDER
        """
        
        try:
            shutil.move(dest, ERROR_DIR / dest.name)
            logger.info(f"Folder moved to error directory: {dest}")
        except Exception as e:
            logger.error(f"Failed to move folder to error directory: {dest}, Error: {e}")
     
        
    def rename_file_for_sc(self, original_filename):
        # Split the filename to separate the main part and the extension
        name_part, extension = original_filename.rsplit('.', 1)

        # Split the name part using underscore as the delimiter
        parts = name_part.split('_')

        if len(parts) > 1:
            # Extract the first letter after the first underscore
            first_letter_after_first_underscore = parts[1][0]

            # Form the new name
            new_name_part = f"{first_letter_after_first_underscore}_{name_part}"
            new_filename = new_name_part + '.' + extension

            return new_filename
        else:
            # If the filename does not meet the requirement (no underscores found)
            return original_filename
    
    def main(self):
        """
        Main function to process inventory and orders data.
        """
        total_images_downloaded = 0
        # Load inventory and order data
        inventories = [inventory for inventory in self.get_inventories()]
        print(len(inventories), " inventory loaded successfully")
        orders = [order for order in self.get_orders()]
        # Process orders, skipping those without SKU
        try:
            for order in orders[6:]:
                # Parse order datetime
                if not order[0]:
                    continue
                try:
                    lastrun_datetime = datetime.strptime(f"{order[0]},{order[1]}", "%d/%m/%Y,%I:%M %p")
                except Exception as e:
                    lastrun_datetime = datetime.strptime(f"{order[0]},{order[1]}", "%Y-%m-%d,%I:%M %p")
      
                # Skip if order datetime is before last run datetime
                if self.get_lastrun() and lastrun_datetime <= self.get_lastrun():
                    continue
                
                print('-------------------------------------------------------------')
                print("Order ID: ",order[2])
                print('-------------------------------------------------------------')
                

                folder = order[2]
                if not order[3]:
                    # If order doesn't have SKU, skip it
                    dest = ERROR_DIR / folder
                    dest.mkdir(parents=True, exist_ok=True)
                    logger.error(f"Order doesn't have SKU: {folder}")
                    error_messages.append(f"Order doesn't have SKU: {folder}")
                    continue

                skus = order[3].split()
                sku_not_in_inventory_count = 0
                for sku in skus:
                    # Find the inventory corresponding to the SKU
                    inventory = next(filter(lambda r: r[0] == sku, inventories), None)

                    if not inventory:
                        sku_not_in_inventory_count += 1
                        continue
                    
                    # Create directory for each order if it doesn't exist
                    dest = DOWNLOAD_DIR / inventory[2] / folder
                    dest.mkdir(parents=True, exist_ok=True)

                    images = []
                    for image in inventory[1].split("|"):
                        image = image.strip()
                        if not image:
                            continue
                        images.append(image)
                        
                    if inventory[2] == "rp" or inventory[2] == "bp":
              
                        bp = inventory[2] == "bp"
                        
                        # Copy files from source to destination
                        for image in images:
                            filename = urlparse(image).path.split("/")[-1]
                            filepath = self.find_file(filename, bp=bp)
                            if not filepath:
                                continue
                            new_filename = f"{order[2]}_{filename}"
                            
                            new_filepath = dest / new_filename
                            shutil.copy(filepath, new_filepath)
                            logger.info(f"Image found at: {filepath}")
                    else:
                        if inventory[2] == "sc":
                            image = images[0]
                            # Download and save images
                            if self.download_img(dest, image, sc="sc"):
                                total_images_downloaded += 1
                                logger.info(f"Total images downloaded: {total_images_downloaded}")
                            else:
                                print("not downloaded file1")
                                sku_not_in_inventory_count += 1
                        else:
                            for image in images:
                                # Download and save images
                                if self.download_img(dest, image):
                                    new_filepath = dest / f"{order[2]}_{urlparse(image).path.split('/')[-1]}"
                                    shutil.move(dest / urlparse(image).path.split('/')[-1], new_filepath)
                                    
                                    total_images_downloaded += 1
                                    logger.info(f"Total images downloaded: {total_images_downloaded}")
                                else:
                                    print("not downloaded file")
                                    sku_not_in_inventory_count += 1
                if sku_not_in_inventory_count == len(skus):
                    try:
                        dest.rmdir()
                        dest = ERROR_DIR / folder
                        logger.error(f"SKU not found in inventory: {folder}")
                        error_messages.append(f"SKU not found in inventory: {folder}")
                        dest.mkdir(parents=True, exist_ok=True)
                    except Exception as e:
                        pass
        finally:
            # Set the last run datetime
            formatted_datetime = lastrun_datetime.strftime("%d/%m/%Y,%I:%M %p")
            #formatted_datetime = "04/01/2023,12:20 PM"
            self.set_lastrun(formatted_datetime)
            
        self.error_folders()

if __name__ == "__main__":
    finder = FileFinder(SEARCH_PATH)
    finder.main()
    
