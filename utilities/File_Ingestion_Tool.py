"""
File Ingestion Tool
-------------------
This script provides a GUI-based interface for loading files into a Spark DataFrame.

Supported File Formats:
- CSV, TXT, XLSX, Parquet, JSON, JDBC, Avro, Hive, Fixed-width

How It Works:
1. User selects the file format, delimiter (if applicable), and other relevant settings via widgets.
2. The script reads the file into a Spark DataFrame.
3. Displays the first few rows of the DataFrame (using `df.show()`).

Note:
- Requires a PySpark environment.
"""

import ipywidgets as widgets                             # For creating a user-friendly GUI
from IPython.display import display                      # To display widgets in Jupyter Notebooks
from pyspark.sql import SparkSession                     # To create a Spark session


# Widgets for user input
data_type = widgets.Dropdown(
    options=['csv', 'txt', 'xlsx', 'parquet', 'json', 'jdbc', 'avro', 'hive', 'fixed-width'],
    description='Data Type:'
)
delimiter_input = widgets.Dropdown(
    options=['comma', 'tilde', 'dash', 'space', 'character position'],
    description='Delimiter:'
)
headers = widgets.RadioButtons(options=['Yes', 'No'], description='Headers?')
split_map_input = widgets.Textarea(description='Split Map:')
file_path_input = widgets.Text(description='File Path:')
file_location = widgets.Dropdown(options=['Local', 'S3', 'HDFS'], description='File Location:')

# Initially hide irrelevant widgets
delimiter_input.layout.visibility = 'hidden'
split_map_input.layout.visibility = 'hidden'

# Update widget visibility based on data type
def on_data_type_change(change):
    delimiter_input.layout.visibility = 'visible' if change['new'] == 'txt' else 'hidden'
    split_map_input.layout.visibility = 'visible' if change['new'] == 'fixed-width' else 'hidden'

data_type.observe(on_data_type_change, names='value')

# Button to submit the input
submit_button = widgets.Button(description='Submit')

# Display the widgets
display(data_type, delimiter_input, headers, split_map_input, file_path_input, file_location, submit_button)

# Function to ingest the file into a Spark DataFrame
def on_button_clicked(b):
    try:
        # Validate user inputs
        if not file_path_input.value:
            print("Error: File path is required.")
            return
        
        if data_type.value == 'fixed-width' and not split_map_input.value:
            print("Error: Split map is required for fixed-width files.")
            return
        
        # Initialize Spark session (lazy initialization)
        print("Initializing Spark session...")
        spark = SparkSession.builder \
            .appName("FileIngestionTool") \
            .getOrCreate()

        header_option = "true" if headers.value == 'Yes' else "false"
        delimiter_map = {'comma': ',', 'tilde': '~', 'dash': '-', 'space': ' ', 'character position': ''}
        delimiter = delimiter_map.get(delimiter_input.value, '')
        file_path = file_path_input.value
        split_map = split_map_input.value

        df = None  # Initialize the DataFrame

        # Read the file into a Spark DataFrame based on the selected data type
        print(f"Loading {data_type.value} file from {file_path}...")
        if data_type.value == 'txt':
            df = spark.read.format("csv") \
                .option("header", header_option) \
                .option("delimiter", delimiter) \
                .load(file_path)

        elif data_type.value == 'csv':
            df = spark.read.format("csv") \
                .option("header", header_option) \
                .load(file_path)

        elif data_type.value == 'xlsx':
            df = spark.read.format("com.crealytics.spark.excel") \
                .option("header", header_option) \
                .option("inferSchema", "true") \
                .load(file_path)

        elif data_type.value == 'parquet':
            df = spark.read.parquet(file_path)

        elif data_type.value == 'json':
            df = spark.read.json(file_path)

        elif data_type.value == 'jdbc':
            # Replace these variables with your actual JDBC connection details
            url = "jdbc:mysql://localhost:3306/my_database"
            table_name = "my_table"
            user = "my_user"
            password = "my_password"
            df = spark.read.format("jdbc") \
                .option("url", url) \
                .option("dbtable", table_name) \
                .option("user", user) \
                .option("password", password) \
                .load()

        elif data_type.value == 'avro':
            df = spark.read.format("avro").load(file_path)

        elif data_type.value == 'hive':
            # Replace this with your actual Hive SQL query
            hive_query = "SELECT * FROM my_table"
            df = spark.sql(hive_query)

        elif data_type.value == 'fixed-width':
            split_map_parsed = [tuple(map(int, item.split('-'))) for item in split_map.split(',')]
            df = spark.read.text(file_path)
            for i, (start, end) in enumerate(split_map_parsed):
                df = df.withColumn(f'field{i+1}', df['value'].substr(start, end-start+1))
            df = df.drop('value')  # Drop the original 'value' column

        # Display the DataFrame
        if df is not None:
            print("File successfully loaded into a Spark DataFrame:")
            df.show(5)  # Show the first 5 rows for verification
        else:
            print("Failed to load file into a DataFrame.")

    except Exception as e:
        print(f"An error occurred while loading the file: {e}")

# Attach the click event handler to the button
submit_button.on_click(on_button_clicked)
