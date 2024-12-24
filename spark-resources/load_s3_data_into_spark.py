"""
Script Name: load_s3_data_into_spark.py
Description:
    This script is designed to facilitate the extraction of data from AWS S3 and loading it into a PySpark DataFrame.
    It initializes a Spark session and a Boto3 S3 client, lists files in a specified S3 bucket directory, filters
    and validates these files based on predefined criteria, and loads the valid files into a Spark DataFrame.
    The script handles configurations for AWS and Spark, and it encapsulates functionality into functions for
    better modularity and reusability.

    The script is structured to be used in data engineering projects where data is frequently ingested from
    S3 into Spark for further processing and analysis. The flexible design allows users to specify bucket names,
    folder paths, and DataFrame schemas as parameters, making the script adaptable for various datasets and
    use cases.

Usage:
    The main function of this script can be called with specific parameters for the S3 bucket name,
    folder name, and DataFrame schema. These parameters can be easily adjusted for different projects
    or data requirements.

    Example:
    python load_s3_data_into_spark.py

Requirements:
    - AWS SDK for Python (Boto3)
    - Apache Spark
    - Python 3.x
    - AWS credentials must be configured (e.g., via AWS CLI or environment variables)

Author: Levi Gagne
Date Created: 2024-06-01
"""

import boto3
import re
from botocore.client import Config
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

def initialize_spark_session():
    """Initializes and returns a Spark session."""
    spark = SparkSession.builder \
        .appName("S3A Test") \
        .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY") \
        .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY") \
        .config("spark.hadoop.fs.s3a.endpoint", "https://s3.amazonaws.com") \
        .getOrCreate()
    print('Spark Version:', spark.version)
    return spark

def get_s3_client():
    """Initializes and returns a boto3 S3 client."""
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        aws_access_key_id="YOUR_ACCESS_KEY",
        aws_secret_access_key="YOUR_SECRET_KEY",
        endpoint_url="https://s3.amazonaws.com",
        config=Config(s3={'addressing_style': 'path'})
    )
    print(f"Boto3 Version: {boto3.__version__}")
    return s3_client

def load_data_from_s3(spark, s3_client, bucket_name, folder_name, df_schema):
    """
    Lists objects in a specified S3 bucket folder, validates them, and loads them into a DataFrame.
    
    Parameters:
        spark (SparkSession): The Spark session instance.
        s3_client (boto3.Client): The boto3 S3 client instance.
        bucket_name (str): The name of the S3 bucket.
        folder_name (str): The name of the folder within the S3 bucket.
        df_schema (StructType): The schema to use when reading the data into DataFrame.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
        if 'Contents' not in response:
            raise FileNotFoundError(f"No files found in s3a://{bucket_name}/{folder_name}")

        valid_files = []
        for obj in response['Contents']:
            file_key = obj['Key']
            file_size = obj['Size']
            if file_size > 0 and re.match(r".*MODEL_NET_.*\.txt$", file_key):
                valid_files.append(f"s3a://{bucket_name}/{file_key}")
            else:
                print(f"Skipping directory or empty file: {file_key}")

        if not valid_files:
            raise FileNotFoundError(f"No valid files found in s3a://{bucket_name}/{folder_name}")

        # Load valid files into DataFrame
        df = spark.read.format('csv') \
            .option("sep", "~") \
            .schema(df_schema) \
            .option("header", "false") \
            .load(valid_files)
        df.show()

    except botocore.exceptions.ClientError as e:
        print("Error accessing S3 bucket:", e)
    except FileNotFoundError as e:
        print(e)
    except Exception as e:
        print(f"Unexpected error: {e}")

def main(bucket_name, folder_name, df_schema):
    spark = initialize_spark_session()
    s3_client = get_s3_client()
    load_data_from_s3(spark, s3_client, bucket_name, folder_name, df_schema)
    spark.stop()

if __name__ == "__main__":
    # Define the schema here, or import it from a schema definition module
    df_schema = StructType([...])  # Replace [...] with your actual schema fields

    # Call main function with specific parameters
    main("fleet-manual-archive", "FLEET_OOS_DATA_ARCHIVE", df_schema)
