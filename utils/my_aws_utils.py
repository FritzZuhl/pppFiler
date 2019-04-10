import datetime
import logging
import os
import pandas as pd
import boto3

# For the given path, get the List of all files in the directory tree
def get_List_Of_Local_Files(dirName):
    # create a list of file and sub directories
    # names in the given directory

    listOfFile = os.listdir(dirName)
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory
        if os.path.isdir(fullPath):
            allFiles = allFiles + get_List_Of_Local_Files(fullPath)
        else:
            allFiles.append(fullPath)
    allFiles = [i.strip('.') for i in allFiles]

    # get non-dir files
    # onlyfiles = [f for f in os.listdir(dirName) if os.path.isfile(os.path.join(dirName, f))]

    # add them
    # allFiles = onlyfiles + allFiles
    return allFiles


# TODO: use regex to include and filter files
# Get local filenames.
# Uses get_List_Of_Local_Files().
# returns relative file paths from dir
def get_filenames(dir, include=None, exclude=None):
    if exclude is None:
        exclude = []
    if include is None:
        file_names = [fn for fn in get_List_Of_Local_Files(dir)]
    else:
        file_names = [fn for fn in get_List_Of_Local_Files(dir) if any(fn.endswith(ext) for ext in include)]

    file_names = [i.replace(dir, "") for i in file_names]
    file_names = [i for i in file_names if i not in exclude]
    resulting_files = [i.strip('/') for i in file_names]
    return resulting_files


def upload(local_file, s3_key, bucket, client_obj):
    # client_obj = boto3.client('s3')
    try:
        with open(local_file, 'rb') as data:
            client_obj.upload_fileobj(data, bucket, s3_key)
            return ""
    except FileNotFoundError:
        raise FileNotFoundError
    except TypeError:
        raise TypeError


def get_existing_keys(bucket_name, prefix='/', delimiter='/'):
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    bucket = boto3.resource('s3').Bucket(bucket_name)
    these_keys = [_.key for _ in bucket.objects.filter(Prefix=prefix)]
    return these_keys

def out_secret_keys(this_user = 'Zuhl1'):
    if this_user == 'norm':  # Norman Plestic
        access = {'ACCESS_KEY_ID': '',
                  'ACCESS_SECRET_KEY': ''}
    else:  # Zuhl1
        access = {'ACCESS_KEY_ID': '',
                  'ACCESS_SECRET_KEY': ''}
    return access


def get_buckets(profile=None, region=None):
    import botocore
    import logging

    if profile is None:
        profile = 'default'
    if region is None:
        region ='us-west-2'

    session = boto3.Session(profile_name=profile, region_name=region)

    s3 = session.client('s3')
    try:
        buckets = s3.list_buckets()
    except botocore.exceptions.ClientError:
        logging.log(logging.ERROR, "AWS Credentials Not Set")
        return None
    except botocore.exceptions.ProfileNotFound:
        logging.log(logging.ERROR, "Profile Not Found")
        raise

    for bucket in buckets['Buckets']:
        print(bucket['Name'])


def filename_log(fname='S3Uploader', fmt='_Date_%Y-%m-%d_Time_H%H-M%M'):
    d = datetime.datetime.now().strftime(fmt).format()
    return fname + d + '.log'

def get_keys(bucket, prefix=''):
    key_gen = get_matching_s3_keys(bucket, prefix)
    res = [x for x in key_gen]
    return res

def download_objects(bucket, S3_prefix, destination_dir):
    # get keys
    keys = get_keys(bucket, S3_prefix)
    file_names = get_filenames(keys)

    file_names_full = destination_dir/file_names


def get_key_generator(bucket, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield key
        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


from pathlib import Path
def make_needed_parents(file_path):
    p = Path(file_path)

    if p.exists():
        return True
    parent_path = p.parents[0]
    if parent_path.exists():
        return True
    else:
        try:
            parent_path.mkdir(parents=True, exist_ok=True)
            return True
        except FileExistsError:
            return True


def upload_S3(**kwargs):

    # Create Logging
    object_log_filename = filename_log(fname=kwargs['upload_log_filename_prefix'])
    local_logger = logging.getLogger()
    local_logger.setLevel(logging.INFO)
    log_file_hander = logging.FileHandler(object_log_filename)
    local_logger.addHandler(log_file_hander)



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
    # included = ['wmv', 'iso', 'ISO', 'avi', 'jpg', 'jpeg', 'bmp', 'png', 'gif', 'txt',
    # 'mp4', 'mp3', 'mpg']
    target_files_stripped = get_filenames(kwargs['local_source_dir'], include=None, exclude=file_log)
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
    pass_over_count = 0
    total_files2upload = len(target_files_stripped)
    logging.log(logging.INFO, "******* Starting to upload %s files", total_files2upload)

    ignore_files = kwargs['ignore_files'] + ['.DS_Store', 'descript.ion']

    #
    for i in range(files2consider):
        this_file, uploaded = tuple(file_log_df.loc[i])
        # if uploaded:
        #     logging.log(logging.INFO, "Local file %s already uploaded." % this_file)
        #     continue

        # Is file on ignore list?
        if this_file in ignore_files:
            logging.log(logging.INFO, "Passing over file %s. On ignore list" % this_file)
            pass_over_count += 1
            continue


        s3key = key_prefix + '/' + this_file  # key on S3
        this_file_path = kwargs['local_source_dir'] + '/' + this_file  # complete path of local file
        logging.log(logging.INFO, "Attempting upload: local file %s. S3 Key: %s" % (this_file, s3key))

        # Check if file/key already exists in S3
        existing_keys = get_existing_keys(kwargs['bucket_name'], prefix=key_prefix)
        if s3key in existing_keys:
            logging.log(logging.INFO, "local file %s already present as key %s \n" % (this_file_path, s3key))
            pass_over_count += 1
            continue
        # End check on redundancy

        try:
            upload(this_file_path, s3key, kwargs['bucket_name'], S3_client)
            logging.log(logging.INFO, "upload success, %d file uploaded, %d files to go.\n" % (i + 1, (total_files2upload - (i + 1))))
        except NameError:
            logging.log(logging.CRITICAL, "Cannot find upload function")
            failed_files += 1
            logging.log(logging.CRITICAL, "File %s could not upload" % this_file)
            continue

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

