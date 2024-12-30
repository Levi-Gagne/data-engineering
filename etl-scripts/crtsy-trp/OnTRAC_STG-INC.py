#!/usr/bin/env python
# coding: utf-8

# ===================================
#   IMPORT REQUIRED MODULES
# ===================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import sys
import re
import fnmatch

# ===================================
#   PARSE THE ENVIRONMENT ARGUMENT
# ===================================
ENV = sys.argv[1]   # Get the environment argument


# ==================================================
#   INITIALIZE SPARK SESSION WITH HIVE SUPPORT
# ==================================================
spark = (
    SparkSession.builder
    .appName("OnTRAC_STG-INC") 
    .enableHiveSupport() 
    .getOrCreate()
)
print('Spark Version: ' + spark.version)

# ===================================
#   SETUP THE AWS S3 CLIENT
# ===================================
session = boto3.session.Session()
s3_client = session.client(
    service_name='s3',
    aws_access_key_id=spark.conf.get("spark.hadoop.fs.s3a.access.key"),
    aws_secret_access_key=spark.conf.get("spark.hadoop.fs.s3a.secret.key"),
    endpoint_url=spark.conf.get("spark.hadoop.fs.s3a.endpoint"),
    config=Config(s3={'addressing_style': 'path'})
)
print("Boto3 Version: {}".format(boto3.__version__))


# ===================================
#   SPECIFY S3 BUCKET INFORMATION
# ===================================
bucket_name = "fleet-manual"
archive_bucket = "fleet-manual-archive"
archive_folder = "OnTRAC_ARCHIVE"

# ==================================================
#   GET CURRENT DATE IN TWO DIFFERENT FORMATS
# ==================================================
current_date = datetime.now()
date_format_mmddyyyy = current_date.strftime("%m%d%Y")
date_format_yyyymmdd = current_date.strftime("%Y%m%d")

# ==================================================
#   LOOK FOR A SPECIFIC FILE IN THE S3 BUCKET
# ==================================================
file_pattern = f"OnTRAC_Out_{date_format_mmddyyyy}_new_{date_format_yyyymmdd}.*\\.txt"
objects = s3_client.list_objects_v2(Bucket=bucket_name)['Contents']
matching_files = [obj['Key'] for obj in objects if re.match(file_pattern, obj['Key'])]

# If no matching files found, raise an exception
if not matching_files:
    raise Exception("No matching files found.")

# Use the first matching file
file_name = matching_files[0]

# ==================================================
#   CHECK IF THE FILE ALREADY EXISTS IN ARCHIVE
# ==================================================
archive_file_path = f"{archive_folder}/{file_name}"
archive_objects = s3_client.list_objects_v2(Bucket=archive_bucket)['Contents']
archive_files = [obj['Key'] for obj in archive_objects]

# If file already in archive, raise an exception
if archive_file_path in archive_files:
    raise Exception("The file is already in the archive.")
else:
    # Copy the file to archive
    s3_client.copy({'Bucket': bucket_name, 'Key': file_name}, archive_bucket, archive_file_path)
    print(f"Copied file to archive: {archive_file_path}")

# ==================================================
#   RUN DATAFRAME OPERATIONS IF NEW FILE IS COPIED
# ==================================================
run_dataframe_operations = archive_file_path not in archive_files

if run_dataframe_operations:
    # Define DataFrame schema
    df_schema = StructType([
        StructField("VIN", StringType()),
        StructField("outDate", StringType()),
        StructField("outMile", StringType()),
        StructField("todayDate", StringType()),
        StructField("outType", StringType()),
        StructField("daysInService", StringType()),
        StructField("loanGreaterThan60", StringType())
    ])

    # Load the specific file into DataFrame
    df = spark.read.format('csv') \
         .option("sep", "|") \
         .schema(df_schema) \
         .option("header", "false") \
         .load(f"s3a://{archive_bucket}/{archive_file_path}")

    # Perform DataFrame transformations
    df = df.withColumn("outDate", to_date(col("outDate"), "MM/dd/yyyy"))
    df = df.withColumn("todayDate", to_date(col("todayDate"), "MM/dd/yyyy"))
    df = df.withColumn("outMile", col("outMile").cast(IntegerType()))
    df = df.withColumn("daysInService", col("daysInService").cast(IntegerType()))

    # Show DataFrame and print its schema
    df.show(5)
    df.printSchema()

# TABLE NAME: Static
TABLE_NAME = "dl_edge_base_dcfpa_171749_base_dcfpa_avista.ret_crtsy_trp_pgm_stg"
# TABLE PATH IS DYNAMIC, BASED ON THE ENVIRONMENT VARIABLE
TABLE_PATH = f"/sync/{ENV.lower()}_42124_edw_hi_b/DL/EDGE_BASE/DCFPA-171749/DCFPA-AVISTA/RET_CRTSY_TRP_PGM_STG/Data"

# WRITE DATA TO TABLE IN HDFS
#############################################################################
try:
    print("Writing to {} started: {}".format(TABLE_NAME, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    df.write.format("parquet").mode("append").save(TABLE_PATH)
    print("Writing to {} completed: {}".format(TABLE_NAME, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
except Exception as e:
    print("Failed to write dataframe to HDFS table {}! Error: ".format(TABLE_NAME), e)

# STOP SPARK SESSION & EXIT SCRIPT
#############################################################################
spark.stop()
sys.exit(0)