import AWS_S3
import pandas as pd

# s3_cli = boto3.client('s3')
# Key_ = 'hoosier1/vid06/xvideos.com_16506afaa0c743611d64ec1cc642a15d.mp4'

prefix='Audio_Entertainment/Box 13/Box13_group01'
this_bucket = 'zuhlbucket1'

csv_file = '/Users/fritzzuhl/Dropbox/pppFiler/some_data.csv'

x = AWS_S3.get_existing_keys(bucket_name = this_bucket, prefix=prefix)
y = [AWS_S3.Key_Info(i, Bucket=this_bucket) for i in x]
y_df = pd.DataFrame(y)

y_df.to_csv(csv_file, index=False, mode='w')

# TODO delete all .DS_Store file on S3

d = pd.read_csv(csv_file)


from sqlalchemy import create_engine



db_file = 'sqlite:///Users/fritzzuhl/Dropbox/pppFilter/some_data.db'
engine = create_engine(db_file)

connection = engine.connect()



