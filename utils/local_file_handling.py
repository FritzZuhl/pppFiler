import os
import datetime

# For the given path, get the List of all files in the directory treeo
# Uses recursive descent
def get_List_Of_Local_Files(dirName):
    # create a list of file and sub directories
    # names in the given directory

    listOfFile = os.listdir(dirName)
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory
        if os.path.isdir(fullPath):
            allFiles = allFiles + get_List_Of_Local_Files(fullPath)
        else:
            allFiles.append(fullPath)
    allFiles = [i.strip('.') for i in allFiles]

    # get non-dir files
    # onlyfiles = [f for f in os.listdir(dirName) if os.path.isfile(os.path.join(dirName, f))]

    # add them
    # allFiles = onlyfiles + allFiles
    return allFiles


# TODO: use regex to include and filter files
# Get local filenames.
# Uses get_List_Of_Local_Files().
# returns relative file paths from dir
def get_filenames(dir, include=None, exclude=None):
    if exclude is None:
        exclude = []
    if include is None:
        file_names = [fn for fn in get_List_Of_Local_Files(dir)]
    else:
        file_names = [fn for fn in get_List_Of_Local_Files(dir) if any(fn.endswith(ext) for ext in include)]

    file_names = [i.replace(dir, "") for i in file_names]
    file_names = [i for i in file_names if i not in exclude]
    resulting_files = [i.strip('/') for i in file_names]
    return resulting_files


def filename_log(fname='S3Uploader', fmt='_Date_%Y-%m-%d_Time_H%H-M%M'):
    d = datetime.datetime.now().strftime(fmt).format()
    return fname + d + '.log'


from pathlib import Path
def make_needed_parents(file_path):
    p = Path(file_path)

    if p.exists():
        return True
    parent_path = p.parents[0]
    if parent_path.exists():
        return True
    else:
        try:
            parent_path.mkdir(parents=True, exist_ok=True)
            return True
        except FileExistsError:
            return True


