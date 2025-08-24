import time
import os
import sys
import requests
import json
from zipfile import ZipFile
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging, format_time



def download_zip_file(logger, url, output_dir):
    response = requests.get(url,stream=True)
    os.makedirs(output_dir, exist_ok=True)
    logger.debug("Downloading start")
    if response.status_code == 200:
        filename = os.path.join(output_dir, "downloaded.zip")
        with open(filename,"wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logger.info(f"Downloaded zip file : {filename}")
        return filename
    else:
        logger.error("Unsuccessfull")
        raise Exception(f"Failed to download file. Status coe {response.status_code}")
    
def extract_zip_file(logger,zip_filename, output_dir):
    logger.info("Extracting the zip file")
    with ZipFile(zip_filename, "r") as zip_file:
        zip_file.extractall(output_dir)
    
    logger.info(f"Extracted files written to : {output_dir}")
    logger.info("Removing the zip file")
    os.remove(zip_filename)
if __name__ == "__main__":

    logger = setup_logging("extract.log")

    if len(sys.argv) < 2:
        logger.error("Extraction path is required")
        logger.error("Exame Usage:")
        logger.error("python3 execute.py /home/prajwal/Data/Extraction")
    else:
        try:
            logger.info("Starting Extraction Engine...")
            EXTRACT_PATH = sys.argv[1]
            KAGGLE_URL = "https://storage.googleapis.com/kaggle-data-sets/1509195/2493966/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20250816%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250816T035552Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=7268abdeae7cacbdea80bed2adf6dac5468d17f9fd7d00888e7e9e5c6a278d32b7bfdeb04e7cc0d778b694b709c3f6bbd7e31a219bff6d86aea4fce592a2564fe91ae91ba2d065842c5f1f1f93bf2a1aa9f3fdd7010f515c48fbd690118f18420edfc2b45c80640d49c06ab8615a07a3219dc51cee7e7c0d8878f05b85772333c1c10423e6b524738f6b7cd306c2eb7dcd4b68dec558a9e723c70066233519c03e89cdaec1316ad38383a3e7415c21ef9abaf78651eed1cdd8837e8d9eb4db62c49e8c1ddab8e6d988ab7c8fabdcd343f8fdb210cea870eeac99898dc6c82b341b4799ecbbda0f6918b32e242e6c4cd23cf756bbb76e06efeb0019074d4def86"
            
            start = time.time()
            
            zip_filename = download_zip_file(logger, KAGGLE_URL,EXTRACT_PATH)
            extract_zip_file(logger, zip_filename, EXTRACT_PATH)
            
            end = time.time()
            logger.info("Extraction Sucessfully Complete!!!")
            logger.info(f"Total time taken {format_time(end-start)}")

        except Exception as e:
            logger.error(f"Error: {e}")