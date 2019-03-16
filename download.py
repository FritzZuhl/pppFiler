import boto3
import botocore
import my_aws_utils
import logging
import sys

config = {
            # Change for each project
            'S3_prefix'                 : 'hoosier2/Drawings',  # where is it in the bucket?
            'destination_key_prefix'    : '',   # what part of S3_prefix do you want to keep?

            # Rarely Changeo
            'download_log_filename_prefix': 'logs/S3download_log',
            'bucket'                    : 'zuhlbucket1',
            'destination_dir'           : '/Users/fritzzuhl/documents'
          }

logging.basicConfig(filename=my_aws_utils.filename_log(fname=config['download_log_filename_prefix']),
                    filemode='w',
                    format="%(asctime)s, Log level: %(levelname)s, msg: %(message)s",
                    level=logging.INFO)

# Get the key ends
# These are complete path keys that belong to a particular prefix.
logging.log(logging.INFO, "****** Starting to download")
try:
    particular_keys = [x for x in my_aws_utils.get_key_generator(config['bucket'], prefix=config['S3_prefix'])]
except KeyError:
    print("There may be a problem with the S3 parameters")
    sys.exit(1)

logging.log(logging.INFO, "Found %d keys in %s." % (len(particular_keys),config['S3_prefix']))

s3 = boto3.resource('s3')

# get the S3 objects
# write them to destination directory
N = len(particular_keys)
for i, this_key in enumerate(particular_keys):
    # add destination_dir to key_ends to create complete path
    dest_complete_path = config['destination_dir'] + '/' + this_key
    my_aws_utils.make_needed_parents(dest_complete_path)
    try:
        s3.Bucket('zuhlbucket1').download_file(this_key, dest_complete_path)
        logging.log(logging.INFO, "Downloaded this key %s. Complete filepath %s" % (this_key,dest_complete_path))
        files_to_go = N - i
        percent_complete = (i/N) * 100
        logging.log(logging.INFO,
                    "Download %d objects. %d objects to go. %.1f%% completed" %
                    (i, files_to_go, percent_complete))
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logging.log(logging.WARNING, "The object %s does not exist." % this_key)
        else:
            raise

logging.log(logging.INFO, "Finished download")