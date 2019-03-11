import boto3
import my_aws_utils
import logging
import pandas as pd
import os
import sys

logging.basicConfig(filename=my_aws_utils.filename_log(fname="logs/S3Upload_log"),
                    filemode='w',
                    format="%(asctime)s, Log level: %(levelname)s, msg: %(message)s",
                    level=logging.INFO)

# File Source
LOCAL_SOURCE_DIR = '/Volumes/LaCie/ppp/Volumes/wave02_archived/Level 4'
# LOCAL_SOURCE_DIR = '/Users/fritzzuhl/testdir'

# S3
BUCKET_NAME = 'zuhlbucket1'
MAJOR_DIR_ON_S3 = 'hoosier3'
DEST_DIR_ON_S3 = 'wave02/Level_4'
key_prefix = MAJOR_DIR_ON_S3 + '/' + DEST_DIR_ON_S3


file_log = 'wave02_level4_file_log.csv'

if not(os.path.exists(LOCAL_SOURCE_DIR)):
    # logging.log(logging.CRITICAL, "Cannot find local source directory %s" % LOCAL_SOURCE_DIR)
    print("Cannot find local source directory %s" % LOCAL_SOURCE_DIR)
    print("Exiting.")
    sys.exit(1)

# Get Selected List of Files to Upload, and Purge file_log
included = ['jpg','jpeg', 'bmp', 'png', 'gif','txt', 'mp4', 'mp3']
excluded = file_log
target_files_stripped = my_aws_utils.get_filenames(LOCAL_SOURCE_DIR, included, excluded)





# file_log = LOCAL_SOURCE_DIR + file_log
if os.path.exists(file_log):
    file_log_df = pd.read_csv(file_log)
else:
    file_log_df = pd.DataFrame(target_files_stripped, columns=['file'])
    file_log_df["uploaded"] = False

# check on shape
files2consider, columns = file_log_df.shape

# Connect to S3
S3_client = boto3.client('s3')

# Begin Upload
uploaded_files_count = 0
failed_files = 0
total_files2upload = len(target_files_stripped)
logging.log(logging.INFO, "******* Starting to upload %s files", total_files2upload)
for i in range(files2consider):
    this_file, uploaded = tuple(file_log_df.loc[i])
    if uploaded:
        logging.log(logging.INFO, "Local file %s already uploaded." % this_file)
        continue

    s3key = key_prefix + '/' + this_file # key on S3
    this_file_path = LOCAL_SOURCE_DIR + '/' + this_file # complete path of local file

    logging.log(logging.INFO, "Attempting upload: local file %s. S3 Key: %s" % (this_file, s3key))
    try:
        my_aws_utils.upload(this_file_path, s3key, BUCKET_NAME, S3_client)
        logging.log(logging.INFO, "upload success, %d file uploaded, %d files to go." % (i,(total_files2upload-i)))
    except NameError:
        logging.log(logging.CRITICAL, "Cannot find upload function")
    except:
        failed_files += 1
        logging.log(logging.CRITICAL, "File %s could not upload" % this_file)
        continue
    uploaded_files_count += 1
    file_log_df.iloc[i,1] = True

logging.log(logging.INFO, "Uploaded %d files" % uploaded_files_count)
if failed_files==0:
    logging.log(logging.INFO, "Uploaded all files.")
else:
    logging.log(logging.INFO, "Failed to upload %d files" % failed_files)

# Finally, write file_log
try:
    file_log_df.to_csv(file_log, index=False)
except OSError:
    file_log_df.to_csv('/Users/fritzzuhl/Dropbox/pppFiler/file_logs/file_log.csv', index=False)

