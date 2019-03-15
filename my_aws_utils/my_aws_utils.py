import os
import boto3
import datetime




'''
For the given path, get the List of all files in the directory tree 
'''
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


