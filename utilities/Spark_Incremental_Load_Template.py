"""
Incremental Load Template
-------------------------
This script provides a template for performing an incremental data load from S3 into a staging table in Hive and then
incrementally updating a target table in Hive. It is designed to be modular, extensible, and robust for various use cases.

Key Features:
- Incremental data load from S3 to Hive
- Configurable schema and S3 parameters
- Comprehensive error handling and logging
- Uses PySpark for distributed data processing

Prerequisites:
- AWS credentials must be configured (or provided via environment variables)
- S3 bucket and folder must exist and be accessible
- Apache Hive must be configured for the Spark session

Author: Levi Gagne
Date:   2024-10-01
"""

# Standard library imports
import os                                          # For accessing environment variables
import re                                          # For regular expressions (e.g., extracting dates from filenames)
import logging                                     # For logging messages
from datetime import datetime                      # For date manipulations

# PySpark imports
from pyspark.sql import SparkSession               # For creating Spark sessions
from pyspark.sql.functions import *                # For DataFrame operations and transformations
from pyspark.sql.types import *                    # For defining custom schemas

# AWS SDK imports
import boto3                                       # AWS SDK for Python
from botocore.client import Config                 # For custom S3 configurations
from botocore.exceptions import ClientError        # For handling S3-related errors


def main():
    """
    Main function to orchestrate the incremental load process.
    - Configures logging
    - Initializes Spark session
    - Loads configurations
    - Fetches the latest CSV from S3
    - Writes data to a staging table
    - Performs incremental load into the target table
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("IncrementalLoad")

    logger.info("Starting Incremental Load Process")

    # Initialize Spark session
    logger.info("Initializing Spark session")
    spark = SparkSession.builder \
        .appName("Incremental_Load") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Load configurations from environment variables or placeholders
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID', '<your_aws_access_key_id>')        # AWS access key ID
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', '<your_aws_secret_access_key>')  # AWS secret access key
    endpoint_url = os.getenv('S3_ENDPOINT_URL', '<your_endpoint_url>')                   # S3 endpoint URL
    bucket_name = os.getenv('S3_BUCKET_NAME', '<your_bucket_name>')                      # S3 bucket name
    folder_name = os.getenv('S3_FOLDER_NAME', '<your_folder_name>')                      # Folder in S3 bucket

    # Configure S3 client
    logger.info("Configuring S3 client")
    s3_client = boto3.client(
        service_name='s3',                           # S3 as the service name
        aws_access_key_id=aws_access_key_id,         # AWS access key ID
        aws_secret_access_key=aws_secret_access_key, # AWS secret access key
        endpoint_url=endpoint_url,                   # S3 endpoint URL
        config=Config(s3={'addressing_style': 'path'})  # Path-style addressing
    )

    # Define schema for the CSV file
    csv_schema = StructType([
        StructField("column1", StringType(), True),  # Replace with actual column names and types
        StructField("column2", IntegerType(), True),
        StructField("column3", DateType(), True)
    ])

    try:
        # Fetch the latest CSV file from S3
        logger.info("Fetching the latest CSV file from S3")
        latest_csv_file = get_latest_csv_file(s3_client, bucket_name, folder_name)
        logger.info(f"Latest CSV file identified: {latest_csv_file}")

        # Load the CSV file into a Spark DataFrame
        logger.info("Loading data into Spark DataFrame")
        df = (
            spark.read.format('csv')                    # Specify the input format (CSV in this case)
                .option("sep", "\t")                    # Field delimiter; replace with "," for standard CSVs
                .option("header", "true")               # Treat the first line as headers
                .option("inferSchema", "false")         # Disable schema inference for better performance
                .option("schema", csv_schema)           # Use a predefined schema
                .option("nullValue", "")                # Specify how nulls are represented
                .option("mode", "FAILFAST")             # Behavior on malformed lines (e.g., PERMISSIVE, DROPMALFORMED)
                .option("dateFormat", "yyyy-MM-dd")     # Format to parse date fields
                .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")  # Format to parse timestamps
                .option("multiLine", "false")           # Set to "true" for multi-line records
                .option("encoding", "UTF-8")            # Specify file encoding (default is UTF-8)
                .option("quote", "\"")                  # Quote character for enclosing fields
                .option("escape", "\\")                 # Escape character for special characters
                .option("comment", "#")                 # Skip lines starting with this character
                .option("ignoreLeadingWhiteSpace", "true")  # Ignore leading whitespaces
                .option("ignoreTrailingWhiteSpace", "true")  # Ignore trailing whitespaces
                .load(f"s3a://{bucket_name}/{latest_csv_file}")  # Path to the input file
        )

        # Write data to the staging table
        staging_table_name = "staging_table"              # Update to match your staging table
        logger.info(f"Writing data to staging table: {staging_table_name}")
        df.write.mode("overwrite").saveAsTable(staging_table_name)
        logger.info("Data successfully written to staging table")

        # Perform incremental load from staging to target table
        target_table_name = "target_table"                # Update to match your target table
        logger.info(f"Performing incremental load to target table: {target_table_name}")
        perform_incremental_load(spark, staging_table_name, target_table_name)

    except Exception as e:
        logger.error(f"An error occurred during the incremental load process: {e}", exc_info=True)

    finally:
        # Stop the Spark session
        spark.stop()
        logger.info("Spark session stopped")


def get_latest_csv_file(s3_client, bucket_name, folder_name):
    """
    Fetch the latest CSV file from the specified S3 bucket and folder.

    Args:
        s3_client: Boto3 S3 client
        bucket_name: S3 bucket name
        folder_name: Folder path in the bucket

    Returns:
        str: Key of the latest CSV file

    Raises:
        RuntimeError: If no CSV files are found or an error occurs
    """
    try:
        # List objects in the S3 folder
        objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
        if 'Contents' not in objects:
            raise FileNotFoundError("No files found in the specified folder")

        # Filter and sort CSV files by date in the filename
        csv_files = [obj['Key'] for obj in objects['Contents'] if obj['Key'].lower().endswith('.csv')]
        if not csv_files:
            raise FileNotFoundError("No CSV files found in the specified folder")

        latest_csv_file = sorted(
            csv_files,
            key=lambda x: datetime.strptime(re.search(r'\d{4}-\d{2}-\d{2}', x).group(), '%Y-%m-%d')
        )[-1]

        return latest_csv_file
    except Exception as e:
        raise RuntimeError(f"Error fetching the latest CSV file: {e}")


def perform_incremental_load(spark, staging_table_name, target_table_name):
    """
    Perform an incremental load from the staging table to the target table.

    Args:
        spark: Spark session
        staging_table_name: Name of the staging table
        target_table_name: Name of the target table

    Raises:
        RuntimeError: If an error occurs during the load
    """
    try:
        # Load data from staging and target tables
        staging_df = spark.table(staging_table_name)
        target_df = spark.table(target_table_name)

        # Identify new records to insert
        new_data_df = staging_df.join(target_df, staging_df.id == target_df.id, "leftanti")

        # Append new records to the target table
        new_data_df.write.mode("append").saveAsTable(target_table_name)
        logging.info("Incremental load completed successfully")
    except Exception as e:
        raise RuntimeError(f"Error during incremental load: {e}")


if __name__ == "__main__":
    main()
