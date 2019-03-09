import boto3
import my_aws_utils
import logging
import pandas as pd
import os
import sys

# log_file_name = my_aws_utils.filename_log(fname="S3Upload_log")
logging.basicConfig(filename=my_aws_utils.filename_log(fname="logs/S3Upload_log"),
                    filemode='w',
                    format="%(asctime)s, Log level: %(levelname)s, msg: %(message)s",
                    level=logging.INFO)

BUCKET_NAME = 'zuhlbucket1'
MAJOR_DIR_ON_S3 = 'test_folder'
DEST_DIR_ON_S3 = ''
LOCAL_SOURCE_DIR = '/Users/fritzzuhl/testdir/'
file_log = 'file_log.csv'

if not(os.path.exists(LOCAL_SOURCE_DIR)):
    # logging.log(logging.CRITICAL, "Cannot find local source directory %s" % LOCAL_SOURCE_DIR)
    print("Cannot find local source directory %s" % LOCAL_SOURCE_DIR)
    print("Exiting.")
    sys.exit(1)

# Get Selected List of Files to Upload, and Purge file_log
included_extensions = ['jpg','jpeg', 'bmp', 'png', 'gif','txt', 'mp4', 'mp3']
target_files= [fn for fn in my_aws_utils.get_List_Of_Local_Files(LOCAL_SOURCE_DIR)
              if any(fn.endswith(ext) for ext in included_extensions)]
target_files_stripped = [i.replace(LOCAL_SOURCE_DIR,"") for i in target_files]
target_files_stripped = [i for i in target_files_stripped if i != file_log]

file_log = LOCAL_SOURCE_DIR + file_log
if os.path.exists(file_log):
    file_log_df = pd.read_csv(file_log)
else:
    file_log_df = pd.DataFrame(target_files_stripped, columns=['file'])
    file_log_df["uploaded"] = False

# check on shape
files2consider, columns = file_log_df.shape

# Connect to S3
the_client = boto3.client('s3')

# Begin Upload
uploaded_files_count = 0
failed_files = 0
logging.log(logging.INFO, "******* Starting to upload %s files", len(target_files_stripped))
for i in range(files2consider):
    this_file, uploaded = tuple(file_log_df.loc[i])
    if uploaded:
        logging.log(logging.INFO, "Local file %s already uploaded." % this_file)
        continue
    s3key = MAJOR_DIR_ON_S3 + '/' + this_file
    logging.log(logging.INFO, "Attempting upload: local file %s. S3 Key: %s" % (this_file, s3key))
    try:
        my_aws_utils.upload(LOCAL_SOURCE_DIR + '/' + this_file, s3key, BUCKET_NAME, the_client)
        logging.log(logging.INFO, "upload success")
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
file_log_df.to_csv(file_log, index=False)

print(LOCAL_SOURCE_DIR)
onlyfiles = [f for f in os.listdir(LOCAL_SOURCE_DIR) if os.path.isfile(os.path.join(LOCAL_SOURCE_DIR, f))]
