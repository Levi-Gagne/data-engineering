"""
Dictionary Generator Module

This module provides a utility for generating configuration dictionaries from CSV files for QA report generation.
The generated dictionaries define the parameters and structure necessary for handling different QA reports,
enabling dynamic configuration of data extraction and processing.

Features:
- Generate configuration dictionaries from CSV input, outlining data ranges, headers, and special conditions.
- Supports dynamic start and stop conditions for data extraction based on CSV configuration.
- Handle exceptions and log generation processes for reliability and traceability.

Classes:
    - DictionaryGenerator: Main class for handling the generation of configuration dictionaries from CSV files.

Dependencies:
    - `os` for file path handling.
    - `csv` for reading CSV files.
    - `re` for regular expression matching.
    - `json` for serializing dictionaries into JSON format.
    - `logging` for tracking events and errors.
    - `datetime` for timestamping log entries.
    - `argparse` for parsing command line arguments.

Usage:
    1. Initialize the `DictionaryGenerator` class with paths for saving dictionaries and log files.
    2. Call `generate_dictionary()` with the path to a CSV file, a dictionary name, and an output file name to create a configuration dictionary.
    3. Use the generated configuration for dynamically configuring QA report processing modules.

Example:
    ```python
    from dictionary_generator import DictionaryGenerator

    # Initialize generator
    generator = DictionaryGenerator(save_path='/path/to/dictionaries', log_file='/path/to/log.txt')

    # Generate dictionary
    generator.generate_dictionary(csv_file_path='path/to/config.csv', dictionary_name='QAReportConfig', output_file_name='qa_report_config.py')
    ```

Author:
    - Levi Gagne
"""
import csv
import re
import json
import os
import sys
import argparse
import logging
from datetime import datetime

class DictionaryGenerator:
    """
    Generates configuration dictionaries from CSV files to facilitate dynamic QA report processing.

    Attributes:
        save_path (str): Path where the configuration dictionaries are saved.
        log_file (str): Path to the log file where generation events are recorded.
    """
    def __init__(self, save_path, log_file):
        """
        Initializes the DictionaryGenerator with a save path and a log file path.

        Args:
            save_path (str): Path where the generated dictionary files will be saved.
            log_file (str): Path to the log file for recording generation events.
        """
        self.save_path = os.path.join(save_path, "QADictionaries")
        os.makedirs(self.save_path, exist_ok=True)
        self.log_file = log_file

        # Set up logging
        logging.basicConfig(filename=self.log_file, level=logging.INFO, format='%(message)s')
    
    @staticmethod
    def extract_columns(range_str):
        """
        Extracts column range from a given string in Excel notation.

        Args:
            range_str (str): A string representing a column range in Excel format.

        Returns:
            str: A string representing the column range in Excel format, or None if the input is invalid.
        """
        if ':' in range_str:
            start_col = re.match(r"([A-Z]+)\d*", range_str.split(':')[0]).group(1)
            end_col = re.match r"([A-Z]+)\d*", range_str.split(':')[1]).group(1)
            return f"{start_col}:{end_col}"
        return None

    @staticmethod
    def parse_range(range_str):
        """
        Parses a string representing a cell range in Excel format into its components.

        Args:
            range_str (str): A string representing a cell range in Excel format.

        Returns:
            tuple: A tuple containing the start column, start row, end column, and end row as elements.
                   Returns None if the input is invalid.
        """
        if ':' in range_str:
            pattern = r"([A-Z]+)(\d+)"
            parts = re.findall(pattern, range_str)
            if len(parts) == 2:
                start_col, start_row = parts[0]
                end_col, end_row = parts[1]
                return start_col, int(start_row), end_col, int(end_row)
        return None

    @staticmethod
    def is_valid(value):
        """
        Checks if the provided string value is non-empty and not only whitespace.

        Args:
            value (str): The string value to check.

        Returns:
            bool: True if the string is valid, False otherwise.
        """
        return value.strip() != ''

    @staticmethod
    def convert_to_python_type(value):
        """
        Converts a string value to a Python data type.

        Args:
            value (str): The string value to convert.

        Returns:
            Any: The converted Python data type (bool, None, or original string).
        """
        if value.lower() == 'true':
            return True
        elif value.lower() == 'false':
            return False
        elif value.lower() == 'null':
            return None
        return value

    def generate_dictionary(self, csv_file_path, dictionary_name, output_file_name):
        """
        Generates a configuration dictionary from a CSV file and saves it to a Python file.

        Args:
            csv_file_path (str): Path to the CSV file containing the configuration.
            dictionary_name (str): Name of the dictionary variable to be used in the generated Python file.
            output_file_name (str): Name of the Python file to save the generated dictionary.
        """
        try:
            config = {}
            sheet_index_counter = {}

            with open(csv_file_path, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    sheet_key = row['Sheet_Key']

                    if sheet_key not in sheet_index_counter:
                        sheet_index_counter[sheet_key] = len(sheet_index_counter) + 1

                    sheet_index = sheet_index_counter[sheet_key]

                    if sheet_index not in config:
                        config[sheet_index] = {
                            'sheet_id': row['Sheet_ID#'],
                            'sheet_name': sheet_key,
                            'dataframes': []
                        }

                    prepend_headers = [h for h in [row['Header1'], row['Header2']] if self.is_valid(h)]

                    dataframe_config = {
                        'dataframe_name': row['Sheet_DataFrame'],
                        'data_range': None,
                        'header_range': None,
                        'skiprows_data': None,
                        'nrows_data': None,
                        'header_rows_skip': None,
                        'nrows_header': None,
                        'use_dynamic_headers': False,
                        'prepend_headers': prepend_headers,
                        'use_dynamic_start_stop': False,
                        'start_header_value': None,
                        'stop_data_value': None,
                        'control_column': None,
                        'end_column': None
                    }

                    if self.is_valid(row['Start']) and self.is_valid(row['Stop']):
                        dataframe_config.update({
                            'start_header_value': self.convert_to_python_type(row['Start']),
                            'stop_data_value': self.convert_to_python_type(row['Stop']),
                            'control_column': row['Start/Stop - Column'],
                            'end_column': row['endColumn'],
                            'use_dynamic_start_stop': True
                        })
                    else:
                        result = self.parse_range(row['Data']) if self.is_valid(row['Data']) else None
                        header_result = self.parse_range(row['dynamicHeaders']) if self.is_valid(row['dynamicHeaders']) else None
                        dataframe_config.update({
                            'data_range': self.extract_columns(row['Data']),
                            'header_range': self.extract_columns(row['dynamicHeaders']),
                            'skiprows_data': result[1] - 1 if result else None,
                            'nrows_data': result[3] - result[1] + 1 if result else None,
                            'header_rows_skip': header_result[1] - 1 if header_result else None,
                            'nrows_header': header_result[3] - header_result[1] + 1 if header_result else None,
                            'use_dynamic_headers': self.convert_to_python_type(row['dynamicHeaders'])
                        })

                    config[sheet_index]['dataframes'].append(dataframe_config)

            config_variable = f"{dictionary_name} = {json.dumps(config, indent=4).replace('true', 'True').replace('false', 'False').replace('null', 'None')}"

            file_path = os.path.join(self.save_path, output_file_name)

            with open(file_path, "w") as file:
                file.write(config_variable)

            init_file_path = os.path.join(self.save_path, "__init__.py")
            if not os.path.exists(init_file_path):
                with open(init_file_path, "w") as init_file:
                    init_file.write(f"from .{os.path.splitext(output_file_name)[0]} import {dictionary_name}")

            directory_path = os.path.dirname(file_path)
            file_name = os.path.basename(file_path)
            print(f"Configuration has been saved to: {directory_path}/{file_name}")

            # Log the generation
            self.log_generation(csv_file_path, file_path, True)
        except Exception as e:
            print(f"An error occurred: {e}")
            self.log_generation(csv_file_path, "", False)

    def log_generation(self, csv_file_path, output_file_path, success):
        """
        Logs the outcome of the dictionary generation process.

        Args:
            csv_file_path (str): Path to the CSV file used in the generation process.
            output_file_path (str): Path where the dictionary was saved.
            success (bool): Flag indicating whether the generation was successful.
        """
        log_entry_number = 1
        if os.path.exists(self.log_file):
            with open(self.log_file, 'r') as log:
                log_entry_number = sum(1 for line in log if line.startswith('1. Dictionary')) + 1

        status = "Update successful" if success else "Update failed"
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_message = (
            f"{log_entry_number}. Dictionary Generated For - {os.path.basename(output_file_path)}\n"
            f"    a. Time ran: {timestamp}\n"
            f"    b. Path saved: {output_file_path}\n"
            f"    c. CSV file used to update dictionary: {csv_file_path}\n"
            f"    d. Update successful: {success}\n"
        )
        with open(self.log_file, 'a') as log:
            log.write(log_message)
        
        logging.info(log_message)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate dictionary from CSV file")
    parser.add_argument("csv_file_path", help="Path to the CSV file")
    parser.add_argument("save_path", help="Path to save the output file")
    parser.add_argument("dictionary_name", help="Name of the dictionary variable")
    parser.add_argument("output_file_name", help="Name of the output file")
    parser.add_argument("--log_file", default="/home/datascientist/CVI/QA/QAValidation/QAResources/QADictionaryGenerator/dictionaryGenerator-LOG.txt", help="Path to the log file")

    args = parser.parse_args()

    generator = DictionaryGenerator(args.save_path, args.log_file)
    generator.generate_dictionary(args.csv_file_path, args.dictionary_name, args.output_file_name)
