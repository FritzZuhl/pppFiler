
import sys
import caffeine
sys.path.append('/Users/fritz/Dropbox/pppFiler/utils')   # use on new macbook
# sys.path.append('/Users/fzuhl/Dropbox/pppFiler/utils') # use on old macbooks
import upload_S3
import local_file_handling
import logging
import pathlib

# The directory to upload
#these_dirs = ['vid_20200202c']

config_template = {
        "top_path"                      : 'vid_20210412_reviewed',
        "local_source_dir"              : '/Users/fritz/Putain_Process/uploads/hoosier1',
        "major_dir_on_s3"               : 'hoosier1',
        "check_existing_keys"           : True,        # does a file-level check

        # don't change often
        "bucket_name"                   : 'zuhlbucket1',
        "upload_log_filename_prefix"    : "logs/S3Upload_log",
        "ignore_files"                  : {'.DS_Store', 'descript.ion'}

}

check_logs = False  # does a directory-level check on redundancy
# this is the path to check
## path = 'logs/directories_uploaded_Date_2019-05-22_Time_H11-M48.log'

local_source_dir = pathlib.Path(config_template['local_source_dir'])
if not local_source_dir.exists():
    print("local source directory does not exists.")
    sys.exit(1)

# check upload logs, and purge directories that are listed within, since they've been uploaded.
# these check prevents redundant directories to be uploaded.
# Whereas the check in upload_S3 checks on the file within the directory.
if check_logs:
    def words_in_text(path):
        with open(path) as handle:
            for line in handle:
                words = line.split()
                if words[0] == 'Completed':
                    yield words[-1]

    completed_dirs = [x for x in words_in_text(path)]

    these_dirs = [x for x in these_dirs if x not in completed_dirs]
# End check upload

# these_dirs.sort(reverse=False)

# Log Setup
dir_log = local_file_handling.filename_log('logs/directories_uploaded')
directory_logger = logging.getLogger()
directory_logger.setLevel(logging.INFO)
log_file_handler = logging.FileHandler(dir_log)

config_list = []
these_dirs = [config_template['top_path']]
for this_dir in (these_dirs):
    this_config = config_template.copy()

    # Construct Configuration Object for this iteration
    this_config['top_path'] = this_config['top_path'].format(this_dir)
    this_config['major_dir_on_s3'] = this_config['major_dir_on_s3'].format(this_dir)
#    config_list.append(this_config)

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

