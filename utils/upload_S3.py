import logging
import time
import os
import boto3
import local_file_handling
import AWS_S3

def upload_S3(**kwargs):
    """
    :param kwargs:
    :return:
    """

    # Establish Logging
    object_log_filename = local_file_handling.filename_log(fname=kwargs['upload_log_filename_prefix'])
    local_logger = logging.getLogger()
    local_logger.setLevel(logging.INFO)
    log_file_hander = logging.FileHandler(object_log_filename)
    local_logger.addHandler(log_file_hander)

    # Get source and destination paths set:
    key_prefix = kwargs['major_dir_on_s3']
    local_dir = kwargs['local_source_dir'] + '/' + kwargs['top_path']

    # Check on the Local Source directory
    if not os.path.exists(local_dir):
        print("Cannot find local source directory %s" % local_dir)
        return

    # Get List of Files to Upload
    # All parents are stripped away, so the parent is lost.
    target_files_1 = local_file_handling.get_filenames(local_dir)
    ignore_files = kwargs['ignore_files'].union({'.DS_Store', 'descript.ion'})
    target_files_2 = [x for x in target_files_1 if x not in ignore_files]
    target_files_2.sort()


    # Connect to S3
    # TODO Throw proper exception if cannot connect.
    S3_client = boto3.client('s3')

    logging.log(logging.INFO, "******* Starting to upload %s files", len(target_files_2))

    if kwargs['check_existing_keys']:
        # Check if file/key already exists in S3
        logging.log(logging.INFO, "Begin downloading existing key list: time %s", time.asctime())
        existing_keys = AWS_S3.get_existing_keys(kwargs['bucket_name'], prefix=key_prefix)
        logging.log(logging.INFO, "Finished downloading existing key list: time %s", time.asctime())

    # TODO add loop that purges files.
    # TODO get size of files
    failed_files = pass_over_count = uploaded_files_count = 0
    total_files2upload = len(target_files_2)
    for i, file2upload in enumerate(target_files_2):
        logging.log(logging.INFO, "Start of Loop. Timestamp: %s\n", time.asctime())

        file2upload_path = local_dir + '/' + file2upload # complete path of local file
        s3key = key_prefix + '/' + kwargs['top_path'] + '/' + file2upload     # complete key on S3

        logging.log(logging.INFO, "Attempting upload: LOCAL_FILE: %s. S3 Key: %s" % (file2upload_path, s3key))

        # Is s3key already present in S3
        if kwargs['check_existing_keys'] and (s3key in existing_keys):
            logging.log(logging.INFO, "local file %s already present as key %s \n" % (file2upload_path, s3key))
            pass_over_count += 1
            continue
        # End check on redundancy

        try:
            AWS_S3.upload(file2upload_path, s3key, kwargs['bucket_name'], S3_client)
            logging.log(logging.INFO, "upload success, %d file uploaded, %d files to go.\n" % (i + 1, (total_files2upload - (i + 1))))
        except NameError:
            logging.log(logging.CRITICAL, "Cannot find upload function")
            failed_files += 1
            logging.log(logging.CRITICAL, "File %s could not upload" % file2upload_path)
            continue

        # TODO add timestamp to when a file is uploaded
        uploaded_files_count += 1

    logging.log(logging.INFO, "Uploaded %d files.\n" % uploaded_files_count)
    if failed_files == 0:
        logging.log(logging.INFO, "Finished with local source directory %s.\n" % kwargs['local_source_dir'])
    else:
        logging.log(logging.INFO, "Failed to upload %d files.\n" % failed_files)

    logging.log(logging.INFO, "Passed over %d files that were listed on ignored file list.\n" % pass_over_count)

