"""
Wheel File Utility Tool

This script provides utility functions for working with Python wheel (.whl) files and directories.

Features:
1. `unpack_wheel`: Unpacks a Python wheel file into a specified or default directory.
2. `zip_folder`: Zips the contents of a folder (including subfolders) into a .zip file.
3. `validate_wheel`: Validates if a file is a proper wheel (.whl) file.
4. `list_wheel_contents`: Lists the contents of a wheel file without extracting.
5. `repack_wheel`: Repackages a modified wheel directory into a new wheel file.
6. `inspect_wheel_metadata`: Inspects and displays metadata from the wheel.
7. `list_dependencies`: Lists dependencies from the wheel's METADATA file.

This script is designed to be used as an example tool for showcasing in a repository.

How to Run:
- Ensure you have Python installed along with the `zipfile` and `os` modules (both are standard).
- Copy the script into a `.py` file or run it directly in a Jupyter Notebook.
- Update the `wheel_path`, `folder_to_zip`, and `zip_file_name` variables with your paths.

Example Usage:
- Unpack a wheel file: `unpack_wheel('path_to_your_wheel_file.whl')`
- Zip a folder: `zip_folder('path_to_your_unpacked_directory', 'path_to_output_zip_file.zip')`
- Validate a wheel file: `validate_wheel('path_to_your_wheel_file.whl')`
- List wheel contents: `list_wheel_contents('path_to_your_wheel_file.whl')`
- Repack a wheel: `repack_wheel('path_to_directory', 'output_wheel_file.whl')`
- Inspect metadata: `inspect_wheel_metadata('path_to_your_wheel_file.whl')`
- List dependencies: `list_dependencies('path_to_your_wheel_file.whl')`

Author: Levi Gagne
"""

import zipfile
import os

def unpack_wheel(wheel_path, extract_to=None):
    """
    Unpack a wheel (.whl) file to a specified directory.

    Args:
        wheel_path (str): Path to the .whl file.
        extract_to (str, optional): Directory to extract the files into. 
                                    If None, extracts to the same directory as the wheel file.
    """
    if extract_to is None:
        extract_to = os.path.splitext(wheel_path)[0]  # Remove the .whl and use as directory name

    with zipfile.ZipFile(wheel_path, 'r') as wheel_file:
        wheel_file.extractall(extract_to)
        print(f"Wheel file extracted to: {extract_to}")

def zip_folder(folder_path, output_path):
    """
    Zip the contents of an entire folder (with its subfolders) into a zip file.

    Args:
        folder_path (str): Path to the folder to be zipped.
        output_path (str): Output zip file path.
    """
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                zipf.write(
                    os.path.join(root, file),
                    os.path.relpath(os.path.join(root, file),
                                    os.path.join(folder_path, '..'))
                )

def validate_wheel(wheel_path):
    """
    Validate if a file is a proper wheel (.whl) file.

    Args:
        wheel_path (str): Path to the wheel file.

    Returns:
        bool: True if the file is a valid wheel, False otherwise.
    """
    return wheel_path.endswith('.whl') and zipfile.is_zipfile(wheel_path)

def list_wheel_contents(wheel_path):
    """
    List the contents of a wheel (.whl) file without extracting.

    Args:
        wheel_path (str): Path to the wheel file.
    """
    with zipfile.ZipFile(wheel_path, 'r') as wheel_file:
        print("Contents of the wheel file:")
        for file_name in wheel_file.namelist():
            print(f"- {file_name}")

def repack_wheel(source_dir, output_wheel):
    """
    Repack a directory into a new wheel (.whl) file.

    Args:
        source_dir (str): Path to the unpacked directory.
        output_wheel (str): Path for the new wheel file.
    """
    with zipfile.ZipFile(output_wheel, 'w', zipfile.ZIP_DEFLATED) as wheel_file:
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, source_dir)
                wheel_file.write(file_path, arcname)
    print(f"New wheel file created: {output_wheel}")

def inspect_wheel_metadata(wheel_path):
    """
    Display the metadata of a wheel (.whl) file.

    Args:
        wheel_path (str): Path to the wheel file.
    """
    with zipfile.ZipFile(wheel_path, 'r') as wheel_file:
        metadata_files = [f for f in wheel_file.namelist() if f.endswith(('METADATA', 'WHEEL'))]
        for metadata_file in metadata_files:
            with wheel_file.open(metadata_file) as meta:
                print(f"--- {metadata_file} ---")
                print(meta.read().decode())

def list_dependencies(wheel_path):
    """
    List dependencies from the METADATA file of a wheel (.whl).

    Args:
        wheel_path (str): Path to the wheel file.
    """
    with zipfile.ZipFile(wheel_path, 'r') as wheel_file:
        metadata_file = next((f for f in wheel_file.namelist() if f.endswith('METADATA')), None)
        if metadata_file:
            with wheel_file.open(metadata_file) as meta:
                for line in meta.read().decode().splitlines():
                    if line.startswith('Requires-Dist'):
                        print(line)
        else:
            print("No METADATA file found in the wheel.")

# Example usages
wheel_path = 'path_to_your_wheel_file.whl'  # Replace with your wheel file path
if validate_wheel(wheel_path):
    unpack_wheel(wheel_path)
    list_wheel_contents(wheel_path)
    inspect_wheel_metadata(wheel_path)
    list_dependencies(wheel_path)
else:
    print(f"Invalid wheel file: {wheel_path}")

folder_to_zip = 'path_to_your_unpacked_wheel_directory'  # Replace with your folder path
zip_file_name = 'path_to_output_zip_file.zip'  # Replace with your desired zip file name
zip_folder(folder_to_zip, zip_file_name)
repack_wheel('path_to_directory', 'new_wheel_file.whl')
