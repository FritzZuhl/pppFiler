import boto3


def get_existing_keys(bucket_name, prefix='/', delimiter='/'):
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    bucket = boto3.resource('s3').Bucket(bucket_name)
    these_keys = [_.key for _ in bucket.objects.filter(Prefix=prefix)]
    return these_keys

def get_keys(bucket, prefix=''):
    key_gen = get_matching_s3_keys(bucket, prefix)
    res = [x for x in key_gen]
    return res

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

