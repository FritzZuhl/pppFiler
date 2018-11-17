import os, string, os.path

TRUE  = 1
FALSE = 0

BASE_DIR = "C:\Users\\fzuhl\Documents\ppp"

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

# ##################################################
# Level 1 Processing
# ##################################################
# MAXDIRSIZE = 52914400  # about 50.4 Mbyte directories
# SOURCE_DIR = "E:\just downloaded"
# RESULT_DIR_BASE = "E:\Level 1\\"
# VOLUME_PREFIX = "L1_"
# VOLSUFFIX_START = 1560


if os.path.exists(SOURCE_DIR):
    os.chdir(SOURCE_DIR)
else:
    print SOURCE_DIR, "does not exists.  Would like to exit but don't know how."
#    return()

# get source dir listing of files
SOURCE_DIRListing = os.listdir(SOURCE_DIR)
SOURCE_DIRListing.sort()

movedFileSize = 0L
currentVolSize = 0L

# make the result base directory if it does not exists
if not os.path.exists(RESULT_DIR_BASE):
    os.mkdir(RESULT_DIR_BASE)

# make the result directory for the first time.
volumSuffix=VOLSUFFIX_START
current_result_dir = RESULT_DIR_BASE + VOLUME_PREFIX + str(volumSuffix)
os.mkdir(current_result_dir)
MadeDir = 1

for i in SOURCE_DIRListing :
    movedFileSize = os.path.getsize(i)
    print "File", i,  "size:", movedFileSize, "in volumn", current_result_dir, ".", "Current volumn size", currentVolSize, "."
   
    currentVolSize = currentVolSize + long(movedFileSize)
    
    oldFile = str(i)
    newfilename = current_result_dir + "/" + oldFile
    os.rename(oldFile,newfilename)
    
    if currentVolSize > MAXDIRSIZE :
        print "____________________________________________________"
        print "Making new volume", volumSuffix + 1
        volumSuffix = volumSuffix + 1
        current_result_dir = RESULT_DIR_BASE + VOLUME_PREFIX + str(volumSuffix)
        os.mkdir(current_result_dir)
        MadeDir = MadeDir + 1
        currentVolSize = 0L    

print "Total number of directories made:", MadeDir
