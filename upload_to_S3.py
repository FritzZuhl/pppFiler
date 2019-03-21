import my_aws_utils
import logging
import os
import sys
import pandas as pd
import boto3

config = {
        # change each project
        "local_source_dir"              : '/Users/fritzzuhl/testdir',
        "destination_dir_s3"            : 'testdir',
        "file_log"                      : 'file_logs/testdir.csv',

        # don't change often
        "major_dir_on_s3"               : 'hoosier1',
        "bucket_name"                   : 'zuhlbucket1',
        "upload_log_filename_prefix"    : "logs/S3Upload_log",
        "force_new_file_list"           : True,
        "reverse_file_order"            : False
}

these_dirs = ['early_L4/L4_00', 'early_L4/L4_001']




def upload_S3(**kwargs):

    # Log Configuration
    logging.basicConfig(filename=my_aws_utils.filename_log(fname=kwargs['upload_log_filename_prefix']),
                        filemode='w',
                        format="%(asctime)s, Log level: %(levelname)s, msg: %(message)s",
                        level=logging.INFO)

    # construct key prefix
    key_prefix = kwargs['major_dir_on_s3'] + '/' + kwargs['destination_dir_s3']

    # File Log - a list of files/keys of concern
    file_log = kwargs['file_log']

    # Check on the Local Source directory
    if not os.path.exists(kwargs['local_source_dir']):
        print("Cannot find local source directory %s" % kwargs['local_source_dir'])
        print("Exiting.")
        return

    # Get Selected List of Files to Upload, and Purge file_log
    included = ['jpg', 'jpeg', 'bmp', 'png', 'gif', 'txt', 'mp4', 'mp3']
    excluded = file_log
    target_files_stripped = my_aws_utils.get_filenames(kwargs['local_source_dir'], included, excluded)
    target_files_stripped.sort(reverse=kwargs['reverse_file_order'])


    # Set File Log
    if os.path.exists(file_log) and not kwargs['force_new_file_list']:
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

    #
    for i in range(files2consider):
        this_file, uploaded = tuple(file_log_df.loc[i])
        if uploaded:
            logging.log(logging.INFO, "Local file %s already uploaded." % this_file)
            continue

        s3key = key_prefix + '/' + this_file  # key on S3
        this_file_path = kwargs['local_source_dir'] + '/' + this_file  # complete path of local file
        logging.log(logging.INFO, "Attempting upload: local file %s. S3 Key: %s" % (this_file, s3key))

        try:
            my_aws_utils.upload(this_file_path, s3key, kwargs['bucket_name'], S3_client)
            logging.log(logging.INFO, "upload success, %d file uploaded, %d files to go." % (i, (total_files2upload-i)))
        except NameError:
            logging.log(logging.CRITICAL, "Cannot find upload function")
            failed_files += 1
            logging.log(logging.CRITICAL, "File %s could not upload" % this_file)
            continue

        uploaded_files_count += 1
        file_log_df.iloc[i, 1] = True

    logging.log(logging.INFO, "Uploaded %d files" % uploaded_files_count)
    if failed_files == 0:
        logging.log(logging.INFO, "Uploaded all files.")
    else:
        logging.log(logging.INFO, "Failed to upload %d files" % failed_files)

    # Finally, write file_log
    try:
        file_log_df.to_csv(file_log, index=False)
    except OSError:
        file_log_df.to_csv('/Users/fritzzuhl/Dropbox/pppFiler/file_logs/file_log.csv', index=False)



upload_S3(**config)

