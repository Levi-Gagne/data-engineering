# dynamic_delimiter_padding.py - Standalone Script to Add Leading Zeros to Numeric Fields
#
# This script processes text files with records separated by a user-specified delimiter.
# It adds leading zeros to numeric fields based on a user-specified length.
#
# Example Input (file content with '~' delimiter):
#   ABC123~42~3~1234~hello
#   XYZ987~7~89~654~world
#
# Example Output (processed file):
#   ABC123~000042~000003~001234~hello
#   XYZ987~000007~000089~000654~world

import os

def add_leading_zeros(value: str, length: int) -> str:
    """
    Add leading zeros to a numeric value based on a specified length.

    Parameters:
        value (str): The input string, expected to be numeric.
        length (int): The total length of the output string.

    Returns:
        str: The value formatted with leading zeros, or the original value if non-numeric.
    """
    try:
        return f"{int(value):0{length}}"
    except ValueError:
        return value

def process_line(line: str, length: int, delimiter: str) -> tuple:
    """
    Process a single line by adding leading zeros to numeric fields.

    Parameters:
        line (str): The line of text to process.
        length (int): The total length of numeric fields after padding.
        delimiter (str): The delimiter used to separate fields.

    Returns:
        tuple: The model name and a list of processed values.
    """
    parts = line.strip().split(delimiter)
    model = parts[0]
    values = [add_leading_zeros(p.strip(), length) for p in parts[1:]]
    return model, values

def read_file(file_path: str) -> list:
    """
    Read all lines from a file.

    Parameters:
        file_path (str): Path to the file to read.

    Returns:
        list: A list of lines from the file.
    """
    with open(file_path, 'r') as file:
        lines = file.readlines()
    return lines

def parse_lines(lines: list, length: int, delimiter: str) -> tuple:
    """
    Parse lines to separate original and processed data.

    Parameters:
        lines (list): Lines from the input file.
        length (int): The total length of numeric fields after padding.
        delimiter (str): The delimiter used to separate fields.

    Returns:
        tuple: Two lists, one for original data and one for processed data.
    """
    original_data = []
    processed_data = []

    for line in lines:
        if line.strip():  # Skip empty lines
            model, values = process_line(line, length, delimiter)
            original_values = [v.strip() for v in line.strip().split(delimiter)[1:]]
            original_data.append((model, original_values))
            processed_data.append((model, values))

    return original_data, processed_data

def sum_columns(data: list) -> list:
    """
    Calculate the sum of numeric values column-wise.

    Parameters:
        data (list): Processed data as a list of rows.

    Returns:
        list: A list of column sums.
    """
    max_columns = max(len(row[1]) for row in data)
    columns_sum = [0] * max_columns

    for row in data:
        values = row[1]
        for i, value in enumerate(values):
            if value.isdigit():  # Only sum numeric values
                columns_sum[i] += int(value)
    return columns_sum

def validate_sums(original_sum: list, processed_sum: list) -> bool:
    """
    Validate if the sums of columns match between original and processed data.

    Parameters:
        original_sum (list): Column sums of the original data.
        processed_sum (list): Column sums of the processed data.

    Returns:
        bool: True if the sums match, False otherwise.
    """
    if original_sum == processed_sum:
        print("Validation passed: The sums of the columns are equal before and after processing.")
        return True
    else:
        print("Validation failed: The sums of the columns are not equal before and after processing.")
        print(f"Original sums: {original_sum}")
        print(f"Processed sums: {processed_sum}")
        return False

def write_file(file_path: str, data: list, delimiter: str) -> None:
    """
    Write processed data to a file.

    Parameters:
        file_path (str): Path to the output file.
        data (list): Processed data to write.
        delimiter (str): The delimiter used to separate fields.
    """
    lines = [f"{model}{delimiter}{delimiter.join(values)}\n" for model, values in data]
    with open(file_path, 'w') as file:
        file.writelines(lines)

def process_file(input_file_path: str, output_dir: str, length: int, delimiter: str) -> bool:
    """
    Process a single file: read, transform, validate, and save.

    Parameters:
        input_file_path (str): Path to the input file.
        output_dir (str): Path to the output directory.
        length (int): The total length of numeric fields after padding.
        delimiter (str): The delimiter used to separate fields.

    Returns:
        bool: True if the file was successfully processed, False otherwise.
    """
    file_name = os.path.basename(input_file_path)
    if not os.path.isfile(input_file_path):
        print(f"File {file_name} not found in {input_file_path}.")
        return False

    print(f"Processing file: {file_name}")
    lines = read_file(input_file_path)
    original_data, processed_data = parse_lines(lines, length, delimiter)

    original_sum = sum_columns(original_data)
    processed_sum = sum_columns(processed_data)

    if validate_sums(original_sum, processed_sum):
        output_file_path = os.path.join(output_dir, file_name)
        write_file(output_file_path, processed_data, delimiter)
        print(f"Processed file saved to {output_file_path}")
        return True
    else:
        print(f"Skipping file due to validation failure: {file_name}")
        return False

def process_directory(input_dir: str, output_dir: str, length: int, delimiter: str) -> None:
    """
    Process all files in a directory.

    Parameters:
        input_dir (str): Path to the input directory.
        output_dir (str): Path to the output directory.
        length (int): The total length of numeric fields after padding.
        delimiter (str): The delimiter used to separate fields.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    files_moved = 0
    files_failed = 0

    for file_name in os.listdir(input_dir):
        input_file_path = os.path.join(input_dir, file_name)
        if os.path.isfile(input_file_path):
            success = process_file(input_file_path, output_dir, length, delimiter)
            if success:
                files_moved += 1
            else:
                files_failed += 1

    print(f"Summary: {files_moved} files processed successfully, {files_failed} files failed validation.")

# Main Execution
if __name__ == "__main__":
    # User-configurable input and output directories, padding length, and delimiter
    input_directory = input("Enter the path to the input directory: ").strip()
    output_directory = input("Enter the path to the output directory: ").strip()
    padding_length = int(input("Enter the number of characters for numeric padding (e.g., 6): "))
    delimiter = input("Enter the delimiter used in the files (e.g., ~): ").strip()

    # Process all files in the directory
    process_directory(input_directory, output_directory, padding_length, delimiter)
