import boto3
import botocore
import my_aws_utils




config = {
            # Change for each project
            'S3_prefix': 'hoosier2/Drawings', # where is it in the bucket?
            'destination_key_prefix': 'Drawings',   # what part of S3_prefix do you want to keep?

            # Rarely Change
            'bucket' : 'zuhlbucket1',
            'distination_dir' : '/Users/fritzzuhl/Dropbox/pppFiler/returns'
          }

a = my_aws_utils.get_keys(config['bucket'], prefix=config['S3_prefix'])

a_gen = get_matching_s3_keys(config['bucket'], prefix=config['S3_prefix'])


], suffix=''):


s3 = boto3.resource('s3')

pic = s3.get_object(Bucket='zuhlbucket1', Key=x_keys[0])['Body']

# get filename at end of key
file_name = found_keys[0].split('/').pop()


try:
    s3.Bucket('zuhlbucket1').download_file(x_keys[0], 'my_local_image.jpg')
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise



