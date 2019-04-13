import logging
import os
import pandas as pd
import boto3
import local_file_handling
import AWS_S3

def upload_S3(**kwargs):
    """
    # Here is an example configuration object
    # config = {
    #         # change each project
    #         "local_source_dir"              : '/Users/fritzzuhl/testdir', # base of local dir
    #         "destination_dir_s3"            : 'testdir',   # the base dir on S3
    #         "file_log"                      : 'file_logs/testdir.csv',  # the name of the file log where each file/key is listed.
    #
    #         # don't change often
    #         "major_dir_on_s3"               : 'hoosier5',
    #         "bucket_name"                   : 'zuhlbucket1',
    #         "upload_log_filename_prefix"    : "logs/S3Upload_log",
    #         "force_new_file_list"           : True,
    #         "reverse_file_order"            : False
    # }
    # my_aws_utils.upload_S3(**config)
    :param kwargs:
    :return:
    """

    # Create Logging
    object_log_filename = local_file_handling.filename_log(fname=kwargs['upload_log_filename_prefix'])
    local_logger = logging.getLogger()
    local_logger.setLevel(logging.INFO)
    log_file_hander = logging.FileHandler(object_log_filename)
    local_logger.addHandler(log_file_hander)

    # construct key prefix, include major and sub-directories within major.
    key_prefix = kwargs['major_dir_on_s3'] + '/' + kwargs['destination_dir_s3']

    # File Log - a list of files/keys of concern
    file_log = kwargs['file_log']

    # Check on the Local Source directory
    if not os.path.exists(kwargs['local_source_dir']):
        print("Cannot find local source directory %s" % kwargs['local_source_dir'])
        print("Exiting.")
        return

    # Get List of Files to Upload
    target_files_stripped = local_file_handling.get_filenames(kwargs['local_source_dir'])
    target_files_stripped.sort(reverse=kwargs['reverse_file_order'])

    # Set File Log
    if os.path.exists(file_log) and not kwargs['force_new_file_list']:
        file_log_df = pd.read_csv(file_log)
    else:
        file_log_df = pd.DataFrame(target_files_stripped, columns=['file'])
        file_log_df["uploaded"] = False

    # check on shape of file log
    files2consider, columns = file_log_df.shape

    # Connect to S3
    # TODO Throw proper exception if cannot connect.
    S3_client = boto3.client('s3')

    # Begin Upload
    uploaded_files_count = 0
    failed_files = 0
    pass_over_count = 0
    total_files2upload = len(target_files_stripped)
    logging.log(logging.INFO, "******* Starting to upload %s files", total_files2upload)

    ignore_files = kwargs['ignore_files'] + ['.DS_Store', 'descript.ion']

    # TODO add loop that purges files.
    # TODO get size of files
    # TODO construct complete s3key
    # TODO contruct data frame with (1) complete local file, (2) complete s3key, (3)

    #
    for i in range(files2consider):
        this_file, uploaded = tuple(file_log_df.loc[i])

        # Is file on ignore list?
        if this_file in ignore_files:
            logging.log(logging.INFO, "Passing over file %s. On ignore list" % this_file)
            pass_over_count += 1
            continue


        s3key = key_prefix + '/' + this_file  # key on S3
        this_file_path = kwargs['local_source_dir'] + '/' + this_file  # complete path of local file
        logging.log(logging.INFO, "Attempting upload: local file %s. S3 Key: %s" % (this_file, s3key))

        # TODO upload_S3 function, move file list purgeing to before for loop.
        # Check if file/key already exists in S3
        existing_keys = AWS_S3.get_existing_keys(kwargs['bucket_name'], prefix=key_prefix)
        if s3key in existing_keys:
            logging.log(logging.INFO, "local file %s already present as key %s \n" % (this_file_path, s3key))
            pass_over_count += 1
            continue
        # End check on redundancy

        try:
            AWS_S3.upload(this_file_path, s3key, kwargs['bucket_name'], S3_client)
            logging.log(logging.INFO, "upload success, %d file uploaded, %d files to go.\n" % (i + 1, (total_files2upload - (i + 1))))
        except NameError:
            logging.log(logging.CRITICAL, "Cannot find upload function")
            failed_files += 1
            logging.log(logging.CRITICAL, "File %s could not upload" % this_file)
            continue

        # TODO add timestamp to when a file is uploaded
        uploaded_files_count += 1
        file_log_df.iloc[i, 1] = True

    logging.log(logging.INFO, "Uploaded %d files.\n" % uploaded_files_count)
    if failed_files == 0:
        logging.log(logging.INFO, "Finished with local source directory %s.\n" % kwargs['local_source_dir'])
    else:
        logging.log(logging.INFO, "Failed to upload %d files.\n" % failed_files)

    logging.log(logging.INFO, "Passed over %d files that were listed on ignored file list.\n" % pass_over_count)

    # Finally, write file_log
    try:
        file_log_df.to_csv(file_log, index=False)
    except OSError:
        file_log_df.to_csv('/Users/fritzzuhl/Dropbox/pppFiler/file_logs/file_log.csv', index=False)

    local_logger.removeHandler(log_file_hander)

