
import boto3

s3BucketName = 'zuhlbucket1'
oldFolderKey = 'hoosier5/video_Level_3_toArchive'
newFolderKey = 'hoosier5/vid_Level3'



session = boto3.Session()
s3 = session.resource('s3')
bucket = s3.Bucket(s3BucketName)
objs = bucket.objects.filter(Prefix=oldFolderKey)

# inspection
# objs_list = [x for x in objs]
# objs_list_keys = [x.key for x in objs]

for object in bucket.objects.filter(Prefix=oldFolderKey):
    srcKey = object.key
    if not srcKey.endswith('/'):
        fileName = srcKey.split('/')[-1]
        destFileKey = newFolderKey + '/' + fileName
        copySource = s3BucketName + '/' + srcKey
        s3.Object(s3BucketName, destFileKey).copy_from(CopySource=copySource)

