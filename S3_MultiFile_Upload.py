import sys
sys.path.append('/Users/fritzzuhl/Dropbox/pppFiler/utils')
import upload_S3
import logging


import caffeine

config = {
        # change each project
        "local_source_dir"              : '/Users/fritzzuhl/testdir',
        "destination_dir_s3"            : 'testdir',
        "file_log"                      : 'file_logs/testdir.csv',

        # don't change often
        "major_dir_on_s3"               : 'hoosier5',
        "bucket_name"                   : 'zuhlbucket1',
        "upload_log_filename_prefix"    : "logs/S3Upload_log",
        "force_new_file_list"           : True,
        "reverse_file_order"            : False,
        "ignore_files"                  : []
}
upload_S3.upload_S3(**config)
sys.exit()

# /Users/fritzzuhl/Putain/group_09

config_template = {
        # change each project
        "local_source_dir"              : '/Users/fritzzuhl/Putain/{}',
        "major_dir_on_s3"               : 'hoosier5',
        "destination_dir_s3"            : '{}',
        "file_log"                      : 'file_logs/{}.csv',

        # don't change often
        "bucket_name"                   : 'zuhlbucket1',
        "upload_log_filename_prefix"    : "logs/S3Upload_log",
        "force_new_file_list"           : True,
        "reverse_file_order"            : False,
        "ignore_files"                  : ['.DS_Store', 'descript.ion']

}
these_dirs = ['group_09', 'group_10']

# Rarely Change
these_dirs.sort()
dir_log = upload_S3.filename_log('logs/directories_uploaded')
directory_logger = logging.getLogger()
directory_logger.setLevel(logging.INFO)
log_file_handler = logging.FileHandler(dir_log)

config_list = []
for i, this_dir in enumerate(these_dirs):
    this_config = config_template.copy()
    this_config['local_source_dir']   = this_config['local_source_dir'].format(this_dir)
    this_config['destination_dir_s3'] = this_config['destination_dir_s3'].format(this_dir)
    this_config['file_log']           = this_config['file_log'].format(this_dir)
    config_list.append(this_config)

    directory_logger.addHandler(log_file_handler)
    logging.log(logging.INFO, "Starting to upload directory %s", this_dir)
    directory_logger.removeHandler(log_file_handler)

    upload_S3.upload_S3(**this_config)

    directory_logger.addHandler(log_file_handler)
    logging.log(logging.INFO, "Completed upload of directory %s", this_dir)
    directory_logger.removeHandler(log_file_handler)


