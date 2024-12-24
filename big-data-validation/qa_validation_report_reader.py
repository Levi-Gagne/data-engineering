"""
QA Validation Report Reader

This module provides functionality to read, process, and summarize QA validation 
reports based on a provided configuration. It is designed to parse and transform 
Excel files containing validation data into structured pandas DataFrames for further 
analysis or reporting.

Features:
- Locate and verify the existence of the QA report file in a specified directory.
- Load data from multiple sheets and ranges as defined in the configuration.
- Handle errors gracefully, logging issues with specific sheets or DataFrames.
- Summarize processing results, including successful and failed DataFrames.

Classes:
    - QAValidationReportReader: Handles the reading and processing of QA validation reports.

Dependencies:
    - `os` for file path handling.
    - `pandas` for data manipulation.
    - `time` for measuring processing time.
    - `IPython.display` for displaying DataFrames interactively.
    - `colorConfig` for styled output.

Usage:
    1. Import the module and initialize the `QAValidationReportReader` class with a configuration.
    2. Call `process_and_summarize()` to process the report and print a summary of the results.
    3. Access the processed DataFrames for downstream analysis.

Example:
    ```python
    from qa_validation_report_reader import QAValidationReportReader

    config = {
        "sheet_1": {
            "sheet_name": "Sheet1",
            "dataframes": [
                {
                    "dataframe_name": "DataFrame1",
                    "data_range": "A:C",
                    "header_range": "A1:C1",
                    "skiprows_data": 1,
                    "nrows_data": 10,
                    "header_rows_skip": 0
                }
            ]
        }
    }
    
    reader = QAValidationReportReader(config=config, directory="path/to/reports")
    reader.process_and_summarize()
    ```
Author:
    - Levi Gagne
"""

import os
import pandas as pd
import time
from typing import Dict, Any
from IPython.display import display
from colorConfig import C

class QAValidationReportReader:
    """
    Reads and processes QA validation reports based on a provided configuration.

    Attributes:
        config: A dictionary containing the configuration for processing the report.
        directory: The directory where the QA report file is located.
        file_path: The path to the QA report file.
    """
    def __init__(self, config: Dict[str, Any], directory: str = '.'):
        """
        Initializes the QAValidationReportReader with configuration and directory.

        Args:
            config: A dictionary specifying the report structure, data ranges, and other parameters.
            directory: Path to the directory containing the report file (default is current directory).
        """
        self.config = config
        self.directory = directory
        self.file_path = self.find_qe_dsr_report_file()  # Locate the report file
        print(f"{C.b}{C.imperialIvory}Found QE file at:{C.r}{C.veryLightBlue}{self.file_path}{C.r}")

    def find_qe_dsr_report_file(self) -> str:
        """
        Finds the QA report file within the specified directory.

        Returns:
            The path to the QA report file.

        Raises:
            FileNotFoundError: If the report file is not found.
        """
        file_path = os.path.join(self.directory, 'QE_DSR_Report.xlsx')
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"{C.b}{C.red}QE DSR Report.xlsx not found in the directory.{C.r}")
        return file_path

    def load_data(self) -> (Dict[str, pd.DataFrame], list):
        """
        Loads data from the QA report file based on the configuration.

        Returns:
            A tuple containing:
            - A dictionary of DataFrames, each keyed by its name.
            - A list of failed records, including error details for each failed load.

        Raises:
            ValueError: If there is a mismatch between data and header lengths.
        """
        xls = pd.ExcelFile(self.file_path)  # Load the Excel file
        dataframes = {}  # To store successfully loaded DataFrames
        failed_records = []  # To track errors

        for sheet_key, sheet_info in self.config.items():
            sheet_name = sheet_info['sheet_name']
            for df_info in sheet_info['dataframes']:
                dataframe_name = df_info['dataframe_name']
                try:
                    # Extract ranges and rows from configuration
                    data_range = df_info['data_range']
                    header_range = df_info['header_range']

                    # Debugging: Print ranges for visibility
                    print(f"Data range for {dataframe_name}: {data_range}")
                    print(f"Header range for {dataframe_name}: {header_range}")

                    # Read data and headers from the Excel file
                    data = pd.read_excel(xls, sheet_name=sheet_name, usecols=data_range,
                                         skiprows=df_info['skiprows_data'], nrows=df_info['nrows_data'], header=None)
                    headers = pd.read_excel(xls, sheet_name=sheet_name, usecols=header_range,
                                            skiprows=df_info['header_rows_skip'], nrows=1, header=None)

                    # Debugging: Print shapes and first rows for verification
                    print(f"Processing DataFrame: {dataframe_name}")
                    print(f"Data shape: {data.shape}")
                    print(f"Headers shape: {headers.shape}")
                    print(f"Headers: {headers.iloc[0].tolist()}")
                    print(f"Data (first row): {data.iloc[0].tolist()}")

                    # Ensure data and headers have the same number of columns
                    if data.shape[1] != headers.shape[1]:
                        raise ValueError(f"Length mismatch: Expected axis has {data.shape[1]} elements, new values have {headers.shape[1]} elements")

                    # Convert headers to string and assign to DataFrame
                    headers = headers.iloc[0].astype(str).values.tolist()
                    data.columns = headers

                    dataframes[dataframe_name] = data  # Add DataFrame to the collection
                except Exception as e:
                    # Log errors for failed DataFrame loads
                    error_message = f"Error processing '{sheet_name}' ({dataframe_name}): {str(e)}"
                    failed_records.append({
                        'dataframe_name': dataframe_name,
                        'sheet_name': sheet_name,
                        'error': str(e)
                    })
        return dataframes, failed_records

    def process_and_summarize(self):
        """
        Processes the report file and prints a summary of results.
        """
        start_time = time.time()  # Record start time

        # Load data
        dataframes, failed_records = self.load_data()

        end_time = time.time()  # Record end time
        processing_time = end_time - start_time  # Calculate processing duration

        # Print summary of the operation
        self.dataframe_summary(dataframes, failed_records, processing_time)

    def dataframe_summary(self, dataframes: Dict[str, pd.DataFrame], failed_records: list, processing_time: float):
        """
        Summarizes the processed DataFrames and any failed records.

        Args:
            dataframes: A dictionary of successfully loaded DataFrames.
            failed_records: A list of records that failed to load, including error details.
            processing_time: Time taken to process the report (in seconds).
        """
        total_dataframes = len(dataframes)
        summary = (
            f"\n{C.b}{C.i}{C.veryLightBlue}Dataframe Processing Summary:{C.r}\n"
            f"{C.b}{C.cornFlowerBlue}- # of Dataframes Consumed:{C.r} '{C.b}{C.seaShell}{total_dataframes}{C.r}'\n"
        )

        # Create a dictionary of sheets and their DataFrames
        sheet_dict = {}
        for sheet_id, sheet_info in self.config.items():
            sheet_name = sheet_info['sheet_name']
            dataframe_names = [df_info['dataframe_name'] for df_info in sheet_info['dataframes']]
            sheet_dict[sheet_name] = dataframe_names

        # Add details for each sheet and DataFrame
        for sheet_idx, (sheet_name, dataframe_names) in enumerate(sheet_dict.items(), 1):
            summary += f"\n{C.b}{sheet_idx}. Sheet: {sheet_name}{C.r}"
            for df_idx, df_name in enumerate(dataframe_names, 1):
                summary += f"\n    {C.b}{sheet_idx}.{df_idx}. DataFrame: {df_name}{C.r}"

        # Add processing time and failed records to summary
        summary += (
            f"\n{C.b}{C.deepLavender}Processing Time (seconds):{C.r} {C.b}{C.veryLightBlue}'{processing_time:.2f}{C.r}'\n"
            f"{C.b}{C.negative}Failed Records:{C.r} {C.b}{C.seaShell}'{len(failed_records)}'{C.r}"
        )

        # Include details of each failed record
        for failed in failed_records:
            summary += (
                f"\n{C.b}Dataframe Name:{C.r} {failed['dataframe_name']}\n"
                f"{C.b}Sheet Name:{C.r} {failed['sheet_name']}\n"
                f"{C.b}Error:{C.r} {failed['error']}"
            )

        # Print the summary
        print(summary)

        # Display successfully processed DataFrames
        for df_name, df in dataframes.items():
            print(f"\n{C.b}{C.veryLightBlue}Displaying DataFrame: '{C.r}{C.b}{C.bahamaBlue}{df_name}{C.r}{C.b}{C.veryLightBlue}'{C.r}")
            display(df)
