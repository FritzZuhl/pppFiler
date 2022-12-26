
# DESTINATION_FOLDER_ON_S3 = 'hoosier1_extension2'   # for videos
DESTINATION_FOLDER_ON_S3 = 'hoosier6'   # for pictures


# Departure Deck on local machine
DEPARTURE_DECK = '/Users/fritz/S3Transfers'


import sys
from typing import List

import caffeine
sys.path.append('/Users/fritz/Dropbox/pppFiler/utils')
import upload_S3
import local_file_handling
import logging
import pathlib

config_template = {
        # change each project
        "local_source_dir"              : DEPARTURE_DECK, # Departure folder
        "major_dir_on_s3"               : DESTINATION_FOLDER_ON_S3,  # receiving dir. within bucket on S3

        # don't change often
        "bucket_name"                   : 'zuhlbucket1',  # Destination Bucket on S3
        "upload_log_filename_prefix"    : "logs/S3Upload_log",
        "ignore_files"                  : {'.DS_Store', 'descript.ion'},

        # old parameters
        "file_name": '',  # leave empty
        "top_path": '{}',  # leave as empty format string.
        "check_existing_keys"           : False,
}

# Check if Local Source Directory exists
local_source_dir = pathlib.Path(config_template['local_source_dir'])
if not local_source_dir.exists():
    print("local source directory in config_template does not exists.")
    sys.exit(1)

# Get List of directories in Local Source Directory
these_dirs: List[str] = [x.stem for x in local_source_dir.iterdir() if x.is_dir()]
these_dirs.sort(reverse=False)

# Log Setup
dir_log = local_file_handling.filename_log('logs/directories_uploaded')
directory_logger = logging.getLogger()
directory_logger.setLevel(logging.INFO)
log_file_handler = logging.FileHandler(dir_log)

config_list = []
for this_dir in these_dirs:
    this_config = config_template.copy()

    # Construct Configuration Object for this iteration
    this_config['top_path'] = this_config['top_path'].format(this_dir)
    #this_config['major_dir_on_s3'] = this_config['major_dir_on_s3'].format(this_dir)
    # config_list.append(this_config)

    # Setup Logging
    directory_logger.addHandler(log_file_handler)
    logging.log(logging.INFO, "Directories to Upload")
    logging.log(logging.INFO, these_dirs)
    logging.log(logging.INFO, "Starting to upload directory %s", this_dir)
    logging.log(logging.INFO, "Dump of Configuration:\n")
    logging.log(logging.INFO, this_config)
    directory_logger.removeHandler(log_file_handler)

    # Upload to S3
    upload_S3.upload_S3(**this_config)

    # Close Logging
    directory_logger.addHandler(log_file_handler)
    logging.log(logging.INFO, "Completed upload of directory %s", this_dir)
    directory_logger.removeHandler(log_file_handler)

