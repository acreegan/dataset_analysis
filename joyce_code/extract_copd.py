import os
import shutil
import zipfile

# specify the directory to search for folders
# dir_path = '/eresearch/lung/jjoh182/COPDgene/COPDgene_data_from_harddisk/COPDGene'
dir_path = '/eresearch/copdgene/jjoh182/COPDGene/'

# specify the name of the file(s) to search for
file_name = 'example.txt'

# specify the destination directory to copy the folders
dest_path = '/path/to/your/destination/directory'

def search_zip_folder(zip_file_path):
    """
    Searches for a file with the specified name in the specified folder within a zip archive.

    Args:
    - zip_file_path (str): the path to the zip archive
    - folder_name (str): the name of the folder to search within
    - file_name (str): the name of the file to search for

    Returns:
    - str or None: the path to the found file within the zip archive, or None if the file was not found
    """
    # specify the path to the output directory
    # output_dir = "/eresearch/lung/jjoh182/COPDgene/COPDgene_data_from_harddisk/Extracted_folders"
    output_dir = "/eresearch/copdgene/jjoh182/COPDGene_extracted"

    # create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    try:
        # open the zip file for reading
        with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
            # get a list of all the files in the zip archive
            zip_files = zip_file.namelist()

            # filter the list to include only the files in the specified folder
            folder_files = [f for f in zip_files if f.endswith("Lobes.mhd")]

            path_within_zip = [os.path.dirname(f) for f in folder_files if "INSP" in f]

            # construct the output path by removing the path within the zip file from the file path
            # output_path = os.path.join(output_dir, folder_files[len(path_within_zip):])

            if len(path_within_zip) == 0:
                print("Folder is empty")

            else:

                for file_path in zip_files:

                    if path_within_zip[0] in file_path:
                        output_path = os.path.join(output_dir,file_path[len(path_within_zip)-1:])

                        if file_path.endswith('/'):
                            os.makedirs(output_path,exist_ok=True)

                        else:
                            # if the file path is a file, extract it to the output directory
                            with zip_file.open(file_path) as zip_file_contents, open(output_path, 'wb') as output_file:
                                output_file.write(zip_file_contents.read())
    except zipfile.BadZipfile:
        print("bad zip file")
    print("Extracted",zip_file_path)

    return

# def extract_zip_subfolder(zip_file_path):


# walk through the directory and search for folders containing the specified file(s)
for root, dirs, files in os.walk(dir_path):
    for f in files:
        print(f)
        insp_folder = search_zip_folder(root+"/"+f)
