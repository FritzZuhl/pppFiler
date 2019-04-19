import AWS_S3
import pandas as pd

# s3_cli = boto3.client('s3')
# Key_ = 'hoosier1/vid06/xvideos.com_16506afaa0c743611d64ec1cc642a15d.mp4'

prefix='/hoosier1'
this_bucket = 'zuhlbucket1'

x = AWS_S3.get_existing_keys(bucket_name = this_bucket, prefix=prefix)
y = [AWS_S3.Key_Info(i, Bucket=this_bucket) for i in x]
y_df = pd.DataFrame(y)

# TODO delete all .DS_Store file on S3

