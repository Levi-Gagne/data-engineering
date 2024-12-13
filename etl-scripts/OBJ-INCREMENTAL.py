#!/usr/bin/env python
# coding: utf-8

#############################################################################
# GUIDE:
# OBJ_BAC, OBJ_REG, OBJ_ZON, OBJ_ARE, OBJ_GMM, OBJ_NAT, OBJ_SEC - INCREMETNAL LOAD
#############################################################################
# 1. Import necessary packages including SparkSession, boto3, pandas, and more.
# 2. Parse arguments to obtain environment.
# 3. Start a Spark session with Hive support.
# 4. Create an S3 client with the appropriate credentials.
# 5. Define a bucket name and the paths to the source and archive folders.
# 6. Define the file types to be processed.
# 7. Define the target HDFS table names and paths for each file type.
# 8. Define the S3 prefixes for each file type.
# 9. Define the positions where the text files should be split for each file type.
# 10. Define the schemas for each file type.
# 11. Loop over each file type and copy the objects from the prefix to the root of the bucket and archive folder while changing the date to YYYYMMDD format and appending the file type to the file name.
# 12. Loop over each file type and load the text files into a DataFrame, filter out header rows, split the value column into separate columns based on the split positions defined earlier, drop the value column, and convert the separate columns to the desired schema.
# 13. Extract year, month, and day from Rpt_Dt, format the date as YYYYMMDD, and cast the columns to the desired schema.
# 14. Append the data to the HDFS table for each file type.
# 15. Loop over each file type and delete files with the 'OBJ_' prefix and ending in '.txt' from the bucket.
# 16. Stop the Spark session and exit the script with exit code 0.


#############################################################################
# IMPORTS
#############################################################################
from pyspark.sql import SparkSession
#Operational
import sys
from datetime import datetime #to parameterize the date time
from pyspark.sql.functions import to_date, col, substring, struct, concat, date_format, lit, expr
from pyspark.sql import functions as sf
from pyspark.sql.types import *
from datetime import datetime
import time, re, io, logging, json, pandas as pd
import boto3                          #ECS Import
from botocore.client import Config    #ECS Import

############################################################################
# PARSING ARGUMENTS
############################################################################
ENV=sys.argv[1]

#############################################################################
# START SPARK
#############################################################################
from pyspark.sql import SparkSession
spark = (
    SparkSession.builder
    .appName("sz92q9")
    .enableHiveSupport() # Enable Hive support
    .config("spark.sql.sources.default", "hive") # Set default data source to Hive
    .getOrCreate()
)
print('Spark Version: ' + spark.version)
print('Pandas Version: ' + pd.__version__)
  
############################################################################
# DEFINE THE AWS S3 Client
############################################################################
session = boto3.session.Session()
s3_client = session.client(
    service_name='s3',
    aws_access_key_id=spark.conf.get("spark.hadoop.fs.s3a.access.key"),
    aws_secret_access_key=spark.conf.get("spark.hadoop.fs.s3a.secret.key"),
    endpoint_url=spark.conf.get("spark.hadoop.fs.s3a.endpoint"),
    config=Config(s3={'addressing_style': 'path'})
)

############################################################################
# DEFINE THE S3 BUCKET & ARCHIVE FOLDER
############################################################################
bucket = "fleet-manual"
OBJ_SOURCE_PATH = ""
OBJ_ARCHIVE_path = 'FILE_PATH'

############################################################################
# DEFINE A LIST OF THE FOUR OBJ_ FILE TYPES
############################################################################
file_types = ["OBJ_BAC", "OBJ_REG", "OBJ_ZON", "OBJ_ARE", "OBJ_GMM", "OBJ_NAT", "OBJ_SEC"]

############################################################################
# DEFINE THE TABLE & PATH OF TARGET TABLE IN HDFS
############################################################################
# Define the HDFS paths for each file type
bac_path = f"/sync/{ENV.lower()}_42124_edw_hi_b/DL/EDGE_BASE/DCFPA-171749/DCFPA-AVISTA/OBJ_BAC/Data"
reg_path = f"/sync/{ENV.lower()}_42124_edw_hi_b/DL/EDGE_BASE/DCFPA-171749/DCFPA-AVISTA/OBJ_REG/Data"
zon_path = f"/sync/{ENV.lower()}_42124_edw_hi_b/DL/EDGE_BASE/DCFPA-171749/DCFPA-AVISTA/OBJ_ZON/Data"
are_path = f"/sync/{ENV.lower()}_42124_edw_hi_b/DL/EDGE_BASE/DCFPA-171749/DCFPA-AVISTA/OBJ_ARE/Data"
gmm_path = f"/sync/{ENV.lower()}_42124_edw_hi_b/DL/EDGE_BASE/DCFPA-171749/DCFPA-AVISTA/OBJ_GMM/Data"
nat_path = f"/sync/{ENV.lower()}_42124_edw_hi_b/DL/EDGE_BASE/DCFPA-171749/DCFPA-AVISTA/OBJ_NAT/Data"
sec_path = f"/sync/{ENV.lower()}_42124_edw_hi_b/DL/EDGE_BASE/DCFPA-171749/DCFPA-AVISTA/OBJ_SEC/Data"

# Define the table names for each file type
bac_table = 'TABLE_NAME'
reg_table = 'TABLE_NAME'
zon_table = 'TABLE_NAME'
are_table = 'TABLE_NAME'
gmm_table = 'TABLE_NAME'
nat_table = 'TABLE_NAME'
sec_table = 'TABLE_NAME'

############################################################################
# DEFINE THE S3 BUCKET & PREFIXES FOR EACH FILE TYPES
############################################################################
# "cvi_prod_manual/fleet-manual"
prefixes = {
    'OBJ_BAC': 'OBJ_BAC',
    'OBJ_REG': 'OBJ_REG',
    'OBJ_ZON': 'OBJ_ZON',
    'OBJ_ARE': 'OBJ_ARE',
	'OBJ_REG': 'OBJ_GMM',
    'OBJ_ZON': 'OBJ_NAT',
    'OBJ_ARE': 'OBJ_SEC'
}
############################################################################
# DEFINE THE SPLIT POSITIONS FOR THE FILE TYPE
############################################################################
splits_map = {
    "OBJ_BAC": [(6, 6), (12, 2), (14, 3), (17, 8), (25, 7)],
    "OBJ_REG": [(10, 2), (12, 2), (14, 3), (17, 8), (25, 7)],
    "OBJ_ZON": [(10, 2), (14, 2), (16, 4), (20, 2), (22, 3), (25, 8), (33, 7)],
    "OBJ_ARE": [(10, 2), (14, 2), (16, 4), (20, 4), (24, 2), (26, 3), (29, 8), (37, 7)],
    "OBJ_GMM": [(10, 2), (14, 2), (16, 4), (20, 4), (24, 4), (28, 4), (32, 4), (36, 4), (40, 4), (44, 4)],
	"OBJ_NAT": [(3, 2), (5, 3), (8, 8), (16, 7)],
    "OBJ_SEC": [(10, 4), (14, 4), (18, 3), (21, 8), (29, 7)]

############################################################################
# SPECIFY THE SCHEMAS - Define a dictionary with the file types and schemas
############################################################################
schemas = {
    "OBJ_BAC": StructType([
        StructField("Biz_Assoc_Id", StringType()),
        StructField("Sell_Src_Cd", StringType()),
        StructField("Brand_Cd", StringType()),
        StructField("Rpt_Dt", StringType()),
        StructField("Obj_Cnt", StringType())
    ]),
    "OBJ_REG": StructType([
        StructField("Region_Cd", StringType()),
        StructField("Sell_Src_Cd", StringType()),
        StructField("Brand_Cd", StringType()),
        StructField("Rpt_Dt", StringType()),
        StructField("Obj_Cnt", StringType())
    ]),
    "OBJ_ZON": StructType([
        StructField("Region_Cd", StringType()),
        StructField("Sec_Cd", StringType()),
        StructField("Zone_Cd", StringType()),
        StructField("Sell_Src_Cd", StringType()),
        StructField("Brand_Cd", StringType()),
        StructField("Rpt_Dt", StringType()),
        StructField("Obj_Cnt", StringType())
    ]),
    "OBJ_ARE": StructType([
        StructField("Region_Cd", StringType()),
        StructField("Sec_Cd", StringType()),
        StructField("Zone_Cd", StringType()),
        StructField("Area_Cd", StringType()),
        StructField("Sell_Src_Cd", StringType()),
        StructField("Brand_Cd", StringType()),
        StructField("Rpt_Dt", StringType()),
        StructField("Obj_Cnt", StringType())
    ])
	"OBJ_GMM": StructType([
        StructField("Region_Cd", StringType()),
        StructField("Sec_Cd", StringType()),
        StructField("Zone_Cd", StringType()),
        StructField("Area_Cd", StringType()),
        StructField("Sell_Src_Cd", StringType()),
        StructField("Brand_Cd", StringType()),
        StructField("Rpt_Dt", StringType()),
        StructField("Obj_Cnt", StringType())
    ])
	"OBJ_NAT": StructType([
        StructField("Region_Cd", StringType()),
        StructField("Sec_Cd", StringType()),
        StructField("Zone_Cd", StringType()),
        StructField("Area_Cd", StringType()),
        StructField("Sell_Src_Cd", StringType()),
        StructField("Brand_Cd", StringType()),
        StructField("Rpt_Dt", StringType()),
        StructField("Obj_Cnt", StringType())
    ])
	"OBJ_SEC": StructType([
        StructField("Region_Cd", StringType()),
        StructField("Sec_Cd", StringType()),
        StructField("Zone_Cd", StringType()),
        StructField("Area_Cd", StringType()),
        StructField("Sell_Src_Cd", StringType()),
        StructField("Brand_Cd", StringType()),
        StructField("Rpt_Dt", StringType()),
        StructField("Obj_Cnt", StringType())
    ])
}


############################################################################
# COPY THE OBJ_ FILE IN FLEET-MANUAL TO THE SAME BUCKET & OBJECTIVES_ARCHIVE
# 		FOLER WITH THE RPT_DT APPENDED TO & CHANGED TO .TXT FILE
############################################################################
# Loop over each file type
for file_type in file_types:
    prefix = prefixes[file_type]
    s3_path = f"s3a://{bucket}/{prefix}"
    print(f"File path for {file_type}: {s3_path}")
    # List all objects with the given prefix
    objects = s3_client.list_objects(Bucket=bucket, Prefix=prefix)
    print(f"Objects for {file_type}:")
    for obj in objects.get('Contents', []):
        print(f"\t{obj['Key']}")
    # Copy each object to the root of the bucket with the file type appended to the file name
    for obj in objects['Contents']:
        key = obj['Key']
        first_row = s3_client.get_object(Bucket=bucket, Key=key)['Body'].read().decode().split('\n')[0]
        print(f"Extracted first row '{first_row}' from {key}")
        # Extract the date string from the first row and convert it to YYYYMMDD format
        date_str = datetime.strptime(first_row[8:18], '%m/%d/%Y').strftime('%Y%m%d')
        print(f"Extracted date string '{first_row[8:18]}' from {key} and converted to {date_str}")
        # Create the new key with the file type and date appended
        new_key = f"{file_type}_{date_str}.txt"
        print(f"New key for {key} is {new_key}")
        # Check if the file already exists in the bucket
        bucket_objects = s3_client.list_objects(Bucket=bucket, Prefix=new_key)
        if 'Contents' in bucket_objects:
            print(f"File {new_key} already exists in the bucket, skipping copy.")
        else:
            # Copy the object to the bucket
            s3_client.copy_object(Bucket=bucket, CopySource=f"{bucket}/{key}", Key=new_key)
            print(f"Copied {key} to {new_key}")
        # Check if the file already exists in the Objectives_Archive folder
        archive_objects = s3_client.list_objects(Bucket=bucket, Prefix=f"{OBJ_ARCHIVE_PATH}/{new_key}")
        if 'Contents' in archive_objects:
            print(f"File {new_key} already exists in the archive, skipping copy.")
        else:
            # Copy the object to the Objectives_Archive folder
            s3_client.copy_object(Bucket=bucket, CopySource=f"{bucket}/{key}", Key=f"{OBJ_ARCHIVE_PATH}/{new_key}")
            print(f"Copied {key} to {OBJ_ARCHIVE_PATH}/{new_key}")


############################################################################
# APPEND THE INCREMENTAL DATE TO THE TABLE IN HDFS
############################################################################
# Loop over each file type
for file_type in file_types:
    # Define the table name and HDFS path for this file type
    if file_type == "OBJ_BAC":
        table_name = bac_table
        table_path = bac_path
    elif file_type == "OBJ_REG":
        table_name = reg_table
        table_path = reg_path
    elif file_type == "OBJ_ZON":
        table_name = zon_table
        table_path = zon_path
    elif file_type == "OBJ_ARE":
        table_name = are_table
        table_path = are_path
	elif file_type == "OBJ_GMM":
        table_name = gmm_table
        table_path = gmm_path
	elif file_type == "OBJ_NAT":
        table_name = nat_table
        table_path = nat_path
	elif file_type == "OBJ_SEC":
        table_name = sec_table
        table_path = sec_path
		
    # Define the split positions for this file type
    splits = splits_map[file_type]
	
    # Load the text files into a DataFrame and filter out the header rows
    print(f"Loading data for {file_type}")
    df = spark.read.text("s3a://{}/{}*.txt".format(bucket, file_type)) \
            .filter(~col("value").startswith("HDR"))
			
    # Split the value column into separate columns
    for i in range(len(splits)):
        start, end = splits[i]
        df = df.withColumn(f"col{i+1}", substring(col("value"), start, end))
		
    # Drop the value column from the dataframe
    df = df.drop("value")
	
    # Convert the separate columns to the desired schema
    schema = schemas[file_type]
    for i in range(len(schema.fields)):
        df = df.withColumnRenamed(f"col{i+1}", schema.fields[i].name)
        df = df.withColumn(schema.fields[i].name, col(schema.fields[i].name).cast(schema.fields[i].dataType))
		
    # Extract year, month, and day from Rpt_Dt and format the date as YYYYMMDD
    df = df.withColumn("Rpt_Dt", to_date(col("Rpt_Dt"), "MMddyyyy"))
    df = df.withColumn("Rpt_Dt", date_format(col("Rpt_Dt"), "yyyyMMdd"))
	
    # Cast the columns to the desired schema
    for field in schema.fields:
        if field.name == "Biz_Assoc_Id":
            df = df.withColumn(field.name, col(field.name).cast(LongType()))
        elif field.name == "Sell_Src_Cd" or field.name == "Obj_Cnt":
            df = df.withColumn(field.name, col(field.name).cast(IntegerType()))
        elif field.name == "Rpt_Dt":
            df = df.withColumn(field.name, to_date(col(field.name), "yyyyMMdd"))
        else:
            df = df.withColumn(field.name, col(field.name).cast(StringType()))
    print(f"{table_name} appended to {table_path}")
    except Exception as e:
        print(f"Error appending {file_type} data: {str(e)}")
		
    # Additional code to print table name and parquet path
    print(f"Table name for {file_type}: {table_name}")
    print(f"Parquet path for {file_type}: {table_path}")

############################################################################
# DELETE THE FILES WITH THE 'OBJ_' PREFIX & ENDING IN '.txt'
############################################################################	
for file_type in prefixes:
    obj_name = prefixes[file_type]
    obj_files = s3_client.list_objects_v2(Bucket=bucket, Prefix=obj_name)
    for obj_file in obj_files.get('Contents', []):
        if obj_file['Key'].endswith('.txt'):
            s3_client.delete_object(Bucket=bucket, Key=obj_file['Key'])
            print(f"{obj_file['Key']} deleted from {bucket}")

spark.stop()
sys.exit(0)