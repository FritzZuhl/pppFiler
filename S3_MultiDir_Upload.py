
import sys
import caffeine
sys.path.append('/Users/fritz/Dropbox/pppFiler/utils')
import upload_S3
import local_file_handling
import logging
import pathlib

config_template = {
        # change each project
        "file_name"                     : '',
        "top_path"                      : 'vid_20200321e', # individual dirs.
        "local_source_dir"              : '/Users/fritz/Downloads/uploads', # dir. of dirs.
        "major_dir_on_s3"               : 'hoosier1',
        "check_existing_keys"           : False,

        # don't change often
        "bucket_name"                   : 'zuhlbucket1',
        "upload_log_filename_prefix"    : "logs/S3Upload_log",
        "ignore_files"                  : {'.DS_Store', 'descript.ion'}

}

# Check if Local Source Directory exists
local_source_dir = pathlib.Path(config_template['local_source_dir'])
if not local_source_dir.exists():
    print("local source directory in config_template does not exists.")
    sys.exit(1)

# Get List of directories in Local Source Directory
these_dirs = [x.stem for x in local_source_dir.iterdir() if x.is_dir()]


# path = 'logs/directories_uploaded_Date_2020-03-21.log'
# def words_in_text(path):
#     with open(path) as handle:
#         for line in handle:
#             words = line.split()
#             if words[0] == 'Completed':
#                 yield words[-1]
#
# completed_dirs = [x for x in words_in_text(path)]
#
# these_dirs = [x for x in these_dirs if x not in completed_dirs]
#
# these_dirs.sort(reverse=False)

# Log Setup
dir_log = local_file_handling.filename_log('logs/directories_uploaded')
directory_logger = logging.getLogger()
directory_logger.setLevel(logging.INFO)
log_file_handler = logging.FileHandler(dir_log)

config_list = []
for i, this_dir in enumerate(these_dirs):
    this_config = config_template.copy()

    # Construct Configuration Object for this iteration
    this_config['top_path'] = this_config['top_path'].format(this_dir)
    this_config['major_dir_on_s3'] = this_config['major_dir_on_s3'].format(this_dir)
    config_list.append(this_config)

    # Setup Logging
    directory_logger.addHandler(log_file_handler)
    logging.log(logging.INFO, "Starting to upload directory %s", this_dir)
    directory_logger.removeHandler(log_file_handler)

    # Upload to S3
    upload_S3.upload_S3(**this_config)

    # Close Logging
    directory_logger.addHandler(log_file_handler)
    logging.log(logging.INFO, "Completed upload of directory %s", this_dir)
    directory_logger.removeHandler(log_file_handler)

