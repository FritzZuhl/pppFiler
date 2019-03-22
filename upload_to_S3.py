import my_aws_utils

# config = {
#         # change each project
#         "local_source_dir"              : '/Users/fritzzuhl/testdir',
#         "destination_dir_s3"            : 'testdir',
#         "file_log"                      : 'file_logs/testdir.csv',
#
#         # don't change often
#         "major_dir_on_s3"               : 'hoosier1',
#         "bucket_name"                   : 'zuhlbucket1',
#         "upload_log_filename_prefix"    : "logs/S3Upload_log",
#         "force_new_file_list"           : True,
#         "reverse_file_order"            : False
# }

config_template = {
        # change each project
        "local_source_dir"              : '/Volumes/LaCie/ppp/Early_L4/{}',
        "destination_dir_s3"            : '{}',
        "file_log"                      : 'file_logs/{}.csv',

        # don't change often
        "major_dir_on_s3"               : 'hoosier1',
        "bucket_name"                   : 'zuhlbucket1',
        "upload_log_filename_prefix"    : "logs/S3Upload_log",
        "force_new_file_list"           : True,
        "reverse_file_order"            : False
}

these_dirs = ['L4_00', 'L4_001', 'L4_002', 'L4_003', 'L4_004']

config_list = []
for i,this_dir in enumerate(these_dirs):
    this_config = config_template.copy()
    this_config['local_source_dir']   = this_config['local_source_dir'].format(this_dir)
    this_config['destination_dir_s3'] = this_config['destination_dir_s3'].format(this_dir)
    this_config['file_log']           = this_config['file_log'].format(this_dir)

    config_list.append(this_config)
    my_aws_utils.upload_S3(**this_config)





