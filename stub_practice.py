import sys
import caffeine
sys.path.append('/Users/fzuhl/Dropbox/pppFiler/utils')
import upload_S3
import local_file_handling
import logging


config_template = {
        # change each project
        "file_name"                     : '',
        "top_path"                      : 'L4_00',
        "local_source_dir"              : '/Volumes/Seagate Backup Plus Drive/ppp/Early_L4',
        "major_dir_on_s3"               : 'hoosier4/Early_L4',
        "check_existing_keys"           : False,

        # don't change often
        "bucket_name"                   : 'zuhlbucket1',
        "upload_log_filename_prefix"    : "logs/S3Upload_log",
        "ignore_files"                  : {'.DS_Store', 'descript.ion'}

}

upload_S3.upload_S3(**config_template)

#
# some_file = '/Volumes/Seagate Backup Plus Drive/ppp/Early_L4/L4_00/L4_068/!!!!!pixxa0a0003.jpg'
#
# some_key = 'hoosier4/L4_00/L4_068/!!!!!pixxa0a0003.jpg'
# S3_client = boto3.client('s3') #
# AWS_S3.

