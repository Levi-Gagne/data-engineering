#!/usr/bin/env python
# coding: utf-8


SCRIPT_NAME = "Fleet_OOS_Stg_HIS"


"""
FLEET_OOS-HIS
Author: Levi Gagne
Date:   2023-06-01

Overview:
---------
This script is designed to perform ETL operations for Fleet Out-of-Stock (OOS) historical data.
It fetches data from an S3 bucket, applies necessary transformations, and writes the processed data to a Hive table.

Sections:
---------
1. Import Libraries:
- Various libraries for Spark, datetime, and AWS S3 operations.

2. Initialize Spark:
- Initialize a SparkSession with Hive support.

3. Environment Variables:
- Parse the command-line argument to set the environment variable `ENV`.

4. AWS S3 Client:
- Initialize AWS S3 client with necessary configurations.

5. Source Bucket and Folder:
- Define the S3 bucket and folder from where the data will be read.

6. Define Schema:
- Explicitly define the schema for the DataFrame to ensure data integrity.

7. Data Ingestion:
- Read data from S3 bucket into a Spark DataFrame.

8. Data Transformation:
- Perform DataFrame transformations like type casting and date manipulations.

9. Write to Hive Table:
- Write the DataFrame to the target Hive table in Parquet format.

10. Stop Spark & Exit:
- Release resources and terminate the Spark session.

Exit Codes:
-----------
0 - Success
"""


# 1. Import Libraries
#############################################################################
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as func
import io, os, re, sys, time, logging
from pyspark.sql.window import Window
import botocore
import boto3                         #ECS Import
from botocore.client import Config   #ECS Import


# 2. Initialize Spark
#############################################################################
spark = SparkSession.builder \
        .appName("FLEET_OOS-HIS") \
        .enableHiveSupport() \
        .getOrCreate()
print(f'Spark Version: {spark.version}')


# 3. Environment Variables
#############################################################################
ENV = sys.argv[1]  # Environment variable (e.g., dev, prod)


# 4. AWS S3 Client
#############################################################################
session = boto3.session.Session()
s3_client = session.client(
    service_name='s3',
    aws_access_key_id=spark.conf.get("spark.hadoop.fs.s3a.access.key"),
    aws_secret_access_key=spark.conf.get("spark.hadoop.fs.s3a.secret.key"),
    endpoint_url=spark.conf.get("spark.hadoop.fs.s3a.endpoint"),
    config=Config(s3={'addressing_style': 'path'})
)
print(f"Boto3 Version: {boto3.__version__}")


# 5. Source Bucket and Folder
#############################################################################
bucket_name = "fleet-manual-archive"    # SET THE S3 BUCKET: 'cvi_dev_admin/fleet-manual-archive'
folder_name = "FLEET_OOS_DATA_ARCHIVE"  # SET THE S3 BUCKET CONTAINING ALL THE FLEET_OOS HISTORICAL DATA: 'cvi_dev_admin/fleet-manual-archive/FLEET_OOS_DATA_ARCHIVE'


# 6. Define Schema
#############################################################################
df_schema = StructType([
    StructField("Model_Nm", StringType()),
    StructField("Jan", StringType()),
    StructField("Feb", StringType()),
    StructField("Mar", StringType()),
    StructField("Apr", StringType()),
    StructField("May", StringType()),
    StructField("Jun", StringType()),
    StructField("Jul", StringType()),
    StructField("Aug", StringType()),
    StructField("Sep", StringType()),
    StructField("Oct", StringType()),
    StructField("Nov", StringType()),
    StructField("Dec", StringType())
])


# 7. Data Ingestion
#############################################################################
df = spark.read.format('csv') \
    .option("sep", "~") \
    .schema(df_schema) \
    .option("header", "false") \
    .load("s3a://{}/{}/MODEL_NET_*.txt".format(bucket_name, folder_name))


# 8. Data Transformation
#############################################################################
# ADD A COLUMN, 'Rpt_Dt' TO APPEND DATE AS: 'MODEL_NET_YYYYMMDD.txt' FROM FILEPATH
df = df.withColumn("Rpt_Dt", regexp_extract(input_file_name(), r"MODEL_NET_(.*)\.txt", 1))
# CAST 'Rpt_Dt' TO DATE TYPE
df = df.withColumn("Rpt_Dt", to_date("Rpt_Dt", "yyyyMMdd"))

# ADD A COLUMN TITLED "SALES_THROUGH_DT" WITH A VALUE OF ONE-CALENDAR-DAY BEFORE "RPT_DT"
df = df.withColumn("Sales_Through_Dt", date_sub("Rpt_Dt", 1))
# CAST 'Sales_Through_Dt' TO DATE TYPE
df = df.withColumn("Sales_Through_Dt", to_date("Sales_Through_Dt", "yyyyMMdd"))

# CAST ALL DATE COLUMNS TO INTEGER TYPE
for month in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]:
    df = df.withColumn(month, col(month).cast(IntegerType()))

# Print the maximum rpt_dt from the DataFrame
max_rpt_dt = df.agg({"rpt_dt": "max"}).collect()[0][0]
print(f"The maximum rpt_dt in the DataFrame being written is: {max_rpt_dt}")

df.printSchema()
df.show()


# 9. Write to Hive Table
#############################################################################
TARGET_TABLE_NAME = "database.fleet_oos_stg"
TARGET_TABLE_PATH = f"/sync/{ENV.lower()}/FLEET_OOS_STG/Data"

print(f"Writing to {TARGET_TABLE_NAME} started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
df.write.format('parquet').mode('overwrite').saveAsTable(TARGET_TABLE_NAME, path=TARGET_TABLE_PATH)
write_success = True
print(f"Writing to {TARGET_TABLE_NAME} completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# CHECK IF THE FLEET_OOS HISTORICAL LOAD TO THE STAGING TABLE WAS SUCCESSFUL
if write_success:
    print(f"Dataframe written to HDFS table {TARGET_TABLE_NAME} successfully!")
else:
    print(f"Failed to write dataframe to HDFS table {TARGET_TABLE_NAME}!")


# 10. Stop Spark & Exit
#############################################################################
spark.stop()
sys.exit(0)
