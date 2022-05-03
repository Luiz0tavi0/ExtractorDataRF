import os
import zipfile


def unzip_unitario(path_to_folder: str, zip_file: str):
    """worker unzips one file"""
    print('extracting... {}'.format(zip_file))

    try:
        with zipfile.ZipFile(os.path.join(path_to_folder, zip_file), 'r') as zip_ref:
            zip_ref.extractall(path_to_folder)
    except zipfile.BadZipFile as err:
        print(err)
        print(f"Filename: {zip_file}")
        return zip_file
