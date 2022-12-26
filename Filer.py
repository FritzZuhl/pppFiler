import os.path
import sys

# BASE_DIR = "/Users/fritz/Downloads/uploads"

# ##################################################
# Level 2 Processing
# ##################################################
# MAXDIRSIZE = 26214400  # 25 Mbyte directories
# SOURCE_DIR      = BASE_DIR + "\\Level 2, pics, toBeArchived"
# RESULT_DIR_BASE = BASE_DIR + "\\Volumes\\Level 2\\"
# VOLUME_PREFIX = "L2_"
# VOLSUFFIX_START = 6247   # did upto L2_6375 on Dec. 27, 2017

# ##################################################
# Level 4 Processing
# ##################################################
##MAXDIRSIZE = 15728640  # 15 Mbyte directories
##SOURCE_DIR      = BASE_DIR + "\\Level 4, toBeArchived\\"
##RESULT_DIR_BASE = BASE_DIR + "\\Volumes\Level 4\\"
##VOLUME_PREFIX = "L4_"
##VOLSUFFIX_START = 1465  # did upto L4_1464 on May 29 2017

# ##################################################
# Level 3 Processing
# ##################################################
# MAXDIRSIZE = 15728640  # 15 Mbyte directories
# SOURCE_DIR      = BASE_DIR + "\\Level 3, ungrouped\\"
# RESULT_DIR_BASE = BASE_DIR + "\\Level 3, grouped\\"                    
# VOLUME_PREFIX   = "L3_"
# VOLSUFFIX_START = 1672   #did up to L3_1708 on Dec. 2, 2015

# ##################################################
# Level 5 Processing
# ##################################################
# MAXDIRSIZE = 15728640  # 15 Mbyte directories
# SOURCE_DIR      = BASE_DIR + "\\Level 5, toBeArchived\\"
# RESULT_DIR_BASE = BASE_DIR + "\\Volumes\Level 5\\"
# VOLUME_PREFIX = "L5_"
# VOLSUFFIX_START = 254   #did L5_253 on Feb. 14, 2018
# SOURCE_DIR = BASE_DIR

# ##################################################
# Level 1 Processing
# ##################################################
# MAXDIRSIZE = 52914400  # about 50.4 Mbyte directories
# SOURCE_DIR = "E:\just downloaded"
# RESULT_DIR_BASE = "E:\Level 1\\"
# VOLUME_PREFIX = "L1_"
# VOLSUFFIX_START = 1560

# ##################################################
# Level 5 year 2021 Processing
# ##################################################
# TODO: use maximum directory file size in terms of number of files, rather than file size.
#   reason: keep auditing in S3 easier.
#   Nov. 4, 2021
MAXDIRSIZE = 15728640  # 15 Mbyte directories
SOURCE_DIR      = "/Users/fritz/Putain/Putain_Process/NextGen_Pics/NextGen_Level5"
RESULT_DIR_BASE = '/Users/fritz/Putain/Putain_Process/NextGen_Pics/NextGen_Level5_folders'
VOLUME_PREFIX = "L5_Y2022_"   # change this when year changes.
VOLSUFFIX_START = 345   # did L5_344 on Nov. 27, 2022

try:
    os.chdir(SOURCE_DIR)
except FileNotFoundError:
    error_message = "Cannot move to source directory {}.".format(SOURCE_DIR)
    print(error_message)
    sys.exit()

# get source dir listing of files
SOURCE_DIRListing = os.listdir(SOURCE_DIR)
SOURCE_DIRListing = [x for x in SOURCE_DIRListing if x != ".DS_Store"]
SOURCE_DIRListing.sort()

# make the result base directory if it does not exists
if not os.path.exists(RESULT_DIR_BASE):
    os.mkdir(RESULT_DIR_BASE)

# make the result directory for the first time.
volumSuffix = VOLSUFFIX_START
current_result_dir = RESULT_DIR_BASE + '/' + VOLUME_PREFIX + str(volumSuffix)
try:
    os.mkdir(current_result_dir)
except FileExistsError as e:
    print("Start Folder Exists.\n")

MadeDir = 1

movedFileSize = 0
currentVolSize = 0
for index, fileName in enumerate(SOURCE_DIRListing):
    movedFileSize = os.path.getsize(fileName)
    log_string = "File Count {}, file name {}, directory {}. Size is: {}".format(index, fileName, current_result_dir, currentVolSize)
    print(log_string)
    currentVolSize = currentVolSize + movedFileSize
    oldFile = str(fileName)
    newfilename = current_result_dir + "/" + oldFile
    os.rename(oldFile,newfilename)
    if currentVolSize > MAXDIRSIZE:
        volumSuffix += 1
        print("_______Making new volume {}________".format(volumSuffix))
        current_result_dir = RESULT_DIR_BASE + '/' + VOLUME_PREFIX + str(volumSuffix)
        os.mkdir(current_result_dir)
        MadeDir += 1
        currentVolSize = 0

print("Total number of directories made:", MadeDir)

