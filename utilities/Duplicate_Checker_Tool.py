# Duplicate_Checker_Tool.py

# IMPORTS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, row_number
from pyspark.sql.window import Window
from IPython.display import display, HTML
import ipywidgets as widgets
import pandas as pd
import pyarrow as pa
import sys, os

# START-SPARK
spark = SparkSession.builder \
        .appName("Check Duplicates") \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# ENABLE ARROW-BASED COLUMNAR DATA TRANSFERS
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Print the versions in a single line
print(f"Spark Version: {spark.version}, Python Version: {sys.version}, Arrow Version: {pa.__version__}")

# FUNCTION TO CHECK DUPLICATES
def check_duplicates(file_path, column_name, specific_value=None):

    # Read the CSV file into a Pandas DataFrame
    pdf = pd.read_csv(file_path)
    print(f"CSV file read successfully with {len(pdf)} rows.")

    # Extract the part of the column name after the last dot
    correct_column_name = None
    for col_name in pdf.columns:
        actual_col_name = col_name.split('.')[-1]  # Take the part after the last dot
        if actual_col_name.lower() == column_name.lower():
            correct_column_name = col_name
            break
    if correct_column_name is None:
        print(f"Column '{column_name}' does not exist in the CSV file.")
        return
    else:
        display(HTML(f"<h3>Checking for duplicates in column: {correct_column_name}</h3>"))

    # CONVERT THE PANDAS DATAFRAME TO A PYSPARK DATAFRAME
    df = spark.createDataFrame(pdf)
    print("Converted to PySpark DataFrame.")

    correct_column_name_with_backticks = f"`{correct_column_name}`" # Use backticks to reference the column name correctly
    duplicates = df.groupBy(col(correct_column_name_with_backticks)).agg(count(col(correct_column_name_with_backticks)).alias("count")) # Group by the specified column and count the occurrences
    duplicates = duplicates.filter(col("count") > 1) # Filter the rows where the count is greater than 1 (indicating duplicates)
    duplicates.show()  # Show the duplicates along with the count

    # If a specific value is provided, find the duplicates for that value
    if specific_value is not None:
        window_spec = Window.orderBy(col(correct_column_name_with_backticks)) # Create a window specification to assign row numbers
        df_with_row_numbers = df.withColumn("row_number", row_number().over(window_spec)) # Add a new column with row numbers
        specific_duplicates = df_with_row_numbers.filter(col(correct_column_name_with_backticks) == specific_value) # Filter the rows with the specific value and show the row numbers
        specific_duplicates.select("row_number").show()
        print(f"The specific value '{specific_value}' is present {specific_duplicates.count()} times.") # Print the count of duplicates for the specific value
        
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

button = widgets.Button(description="Check Duplicates")

# FUNCTION TO HANDLE THE BUTTON CLICK
def on_button_click(b):
    file_path = file_path_widget.value
    column_name = column_name_widget.value
    check_duplicates(file_path, column_name)

button.on_click(on_button_click)


specific_value_widget = widgets.Text(
    value='',
    placeholder='Enter a specific value (Optional)',
    description='Specific Value:',
    disabled=False
)

output = widgets.Output()

# FUNCTION TO HANDLE THE BUTTON CLICK
def on_button_click(b):
    file_path = "file:///home/datascientist/" + file_path_widget.value
    column_name = column_name_widget.value
    specific_value = specific_value_widget.value if specific_value_widget.value else None
    check_duplicates(file_path, column_name, specific_value)

# Display the output widget along with the others
display(file_path_widget, column_name_widget, specific_value_widget, button, output)