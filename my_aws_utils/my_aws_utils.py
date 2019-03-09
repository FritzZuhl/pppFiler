import boto3
import os

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


import datetime

def filename_log(fname='S3Uploader', fmt='_Date_%Y-%m-%d_Time_H%H-M%M'):
    d = datetime.datetime.now().strftime(fmt).format()
    return fname + d + '.log'

