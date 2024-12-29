"""
# Duplicate Checker Tool

## Overview:
This script is designed to identify and display duplicate entries within a specified column of a CSV file. 
The tool leverages PySpark for its distributed processing capabilities, enabling efficient handling of large datasets. 
Additionally, the script integrates interactive widgets for user-friendly input in environments such as Jupyter Notebook.

## Workflow:
The script follows these steps to perform duplicate detection:
1. **Setup PySpark Session**: Initializes a PySpark session for distributed data processing and configures it for optimized performance using Apache Arrow for columnar data transfers.
2. **Input File Handling**:
   - Reads the provided CSV file into a Pandas DataFrame.
   - Automatically resolves potential discrepancies in column names (e.g., case sensitivity or prefixed column names).
3. **Data Conversion**:
   - Converts the Pandas DataFrame into a PySpark DataFrame for scalable processing.
4. **Duplicate Detection**:
   - Groups the data by the specified column and counts the occurrences of each unique value.
   - Filters the results to display only the rows where duplicates exist (i.e., count > 1).
5. **Specific Value Filtering** (Optional):
   - If a specific value is provided, filters the dataset to show all rows containing that value along with row numbers for identification.
6. **Interactive User Interface**:
   - Provides text input fields for the CSV file path, column name, and an optional specific value.
   - Includes a button to trigger the duplicate detection process, with results displayed in the notebook.

## Features:
- **Scalability**: Utilizes PySpark for efficient handling of large datasets.
- **Interoperability**: Employs Arrow-based data transfers to ensure smooth conversion between Pandas and PySpark.
- **Error Handling**: Validates the existence of the specified column and provides meaningful error messages.
- **Interactive Widgets**: Simplifies input and enhances usability for non-programmatic users.
- **Customization**: Allows filtering for specific values to identify duplicates more precisely.

## Use Cases:
- Detecting duplicate entries in transactional logs, customer data, or other datasets.
- Auditing and cleaning data pipelines before downstream processing.
- Verifying data integrity for ETL processes.

## Required Libraries:
- `pyspark`: For distributed data processing.
- `pandas`: For initial data manipulation.
- `ipywidgets`: For interactive user inputs.
- `pyarrow`: For Arrow-based columnar data transfers.

"""

# IMPORTS
from pyspark.sql import SparkSession  # Spark session for PySpark operations
from pyspark.sql.functions import col, count, row_number  # Functions for column operations and aggregation
from pyspark.sql.window import Window  # For creating window specifications
from IPython.display import display, HTML  # For displaying HTML elements
import ipywidgets as widgets  # Interactive widgets for user input
import pandas as pd  # Data manipulation library
import pyarrow as pa  # Arrow-based columnar data transfer
import sys, os  # System operations

# START-SPARK
# Initialize a PySpark session for distributed data processing
spark = SparkSession.builder \
        .appName("Check Duplicates") \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')  # Set log level to reduce verbosity

# ENABLE ARROW-BASED COLUMNAR DATA TRANSFERS
# Enable optimization for Pandas and PySpark interoperability
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Print the Spark, Python, and Arrow versions
print(f"Spark Version: {spark.version}, Python Version: {sys.version}, Arrow Version: {pa.__version__}")

# FUNCTION TO CHECK DUPLICATES
def check_duplicates(file_path, column_name, specific_value=None):
    """
    Checks for duplicate entries in a specified column of a CSV file.

    Parameters:
    - file_path (str): Path to the CSV file.
    - column_name (str): Name of the column to check for duplicates.
    - specific_value (str, optional): Specific value to check duplicates for, if provided.

    Returns:
    - None: Outputs results to the console and/or displays relevant widgets.
    """
    
    # Read the CSV file into a Pandas DataFrame
    pdf = pd.read_csv(file_path)
    print(f"CSV file read successfully with {len(pdf)} rows.")

    # Resolve the correct column name by ignoring case and potential prefixes
    correct_column_name = None
    for col_name in pdf.columns:
        actual_col_name = col_name.split('.')[-1]  # Extract column name after the last dot
        if actual_col_name.lower() == column_name.lower():
            correct_column_name = col_name
            break
    if correct_column_name is None:
        print(f"Column '{column_name}' does not exist in the CSV file.")
        return
    else:
        display(HTML(f"<h3>Checking for duplicates in column: {correct_column_name}</h3>"))

    # Convert the Pandas DataFrame to a PySpark DataFrame
    df = spark.createDataFrame(pdf)
    print("Converted to PySpark DataFrame.")

    # Perform duplicate detection using PySpark
    correct_column_name_with_backticks = f"`{correct_column_name}`"  # Handle column names with special characters
    duplicates = df.groupBy(col(correct_column_name_with_backticks)) \
                   .agg(count(col(correct_column_name_with_backticks)).alias("count"))  # Count occurrences
    duplicates = duplicates.filter(col("count") > 1)  # Filter rows with duplicates
    duplicates.show()  # Display duplicate values and their counts

    # If a specific value is provided, find duplicates for that value
    if specific_value is not None:
        window_spec = Window.orderBy(col(correct_column_name_with_backticks))  # Define row numbering window
        df_with_row_numbers = df.withColumn("row_number", row_number().over(window_spec))  # Add row numbers
        specific_duplicates = df_with_row_numbers.filter(col(correct_column_name_with_backticks) == specific_value)  # Filter by value
        specific_duplicates.select("row_number").show()  # Display rows with the specific value
        print(f"The specific value '{specific_value}' is present {specific_duplicates.count()} times.")  # Print duplicate count

# WIDGETS FOR USER INPUT
file_path_widget = widgets.Text(
    value='',
    placeholder='Enter the relative path to the CSV file (file:///home/datascientist/ will be added as a prefix)',
    description='File Path:',
    disabled=False
)

column_name_widget = widgets.Text(
    value='',
    placeholder='Enter the column name',
    description='Column Name:',
    disabled=False
)

specific_value_widget = widgets.Text(
    value='',
    placeholder='Enter a specific value (Optional)',
    description='Specific Value:',
    disabled=False
)

button = widgets.Button(description="Check Duplicates")

# FUNCTION TO HANDLE THE BUTTON CLICK
def on_button_click(b):
    """
    Handles the button click event to invoke the duplicate check function.

    Parameters:
    - b: Button click event object.

    Returns:
    - None: Outputs results to the widgets display.
    """
    file_path = "file:///home/datascientist/" + file_path_widget.value  # Prepend prefix to file path
    column_name = column_name_widget.value
    specific_value = specific_value_widget.value if specific_value_widget.value else None
    check_duplicates(file_path, column_name, specific_value)

button.on_click(on_button_click)

# DISPLAY WIDGETS
# Show widgets for user interaction
output = widgets.Output()
display(file_path_widget, column_name_widget, specific_value_widget, button, output)
