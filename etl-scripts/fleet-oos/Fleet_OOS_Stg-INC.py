#!/usr/bin/env python
# coding: utf-8

"""
FLEET_OOS_INC Script

1. Import necessary libraries and modules for Spark, datetime, AWS S3, and logging.
   The script uses multiple Python libraries. Spark for distributed data processing, datetime for handling dates and times, AWS S3 for accessing the Amazon S3 service, 
   and logging for capturing runtime events for debugging or information purposes.

2. Parse the environment parameter passed as an argument to the script.
   This is for customization according to different running environments, e.g. test environment vs. production environment.

3. Start a SparkSession with Hive support.
   A SparkSession is the entry point to use Spark's DataFrame and SQL API. Hive support is enabled for using Hive's built-in functions and Hive tables.

4. Set up the AWS S3 client using the AWS access credentials from Spark configuration.
   This step allows the script to interact with Amazon's S3 service for storing and retrieving files.

5. Specify the 'fleet-manual' S3 bucket as the source bucket.
   This bucket is where the source data file is stored.

6. Specify the source file to be copied as 'MODEL_NET.dat'.
   This is the file that contains the data to be loaded into the Hive table.

7. Get the current date and time in Eastern Timezone (EST).
   The current date and time are needed to rename the source file and to calculate the 'Sales_Through_Dt' column in the DataFrame.

8. Check if today is Monday. If so, exit the script.
   This step is to prevent the script from running on Mondays.

9. Generate the destination file name by appending the current date in the format 'MODEL_NET_YYYYMMDD.txt'.
   This is the name of the file after it is copied and the date is appended to it.

10. Create an S3 client object using the AWS access credentials.
    This object is used to interact with the S3 service for operations such as copying and deleting files.

11. Check if the destination file already exists in the 'fleet-manual' bucket.
    - If it exists, skip the copy operation and print a message.
    - If it doesn't exist, copy the source file to the bucket with the new name.
    This step ensures that the source file is only copied if it does not already exist in the destination bucket, to avoid overwriting existing data.
    
12. Define the schema for the input 'MODEL_NET' files.
    The schema specifies the structure of the data, including column names and data types.
    
13. Load the incremental 'MODEL_NET_YYYYMMDD.txt' file into a Spark DataFrame.
    - Read the CSV file from the 'fleet-manual' bucket, apply the schema, and set the delimiter.
    The DataFrame is a data structure provided by Spark that allows manipulation and analysis of large datasets.
    
14. Add a column 'Rpt_Dt' to the DataFrame with the current date.
    - Cast 'Rpt_Dt' to a date type.
    This step adds a new column to the DataFrame that records the date when the data was loaded.

15. Add a column 'Sales_Through_Dt' with a value of one calendar day before 'Rpt_Dt'.
    - Cast 'Sales_Through_Dt' to a date type.
    This step adds another new column to the DataFrame, recording the day before the data was loaded.

16. Cast all date columns to an integer type.
    This is for data consistency, making sure all date columns are in the same data type.

17. Define the target Hive table 'dl_edge_base_dcfpa_171749_base_dcfpa_avista.fleet_oos_stg'.
    This is the name of the Hive table where the transformed data will be loaded.

18. Use Hive's built-in function to create a partitioned table.
    Partitioning is a method of dividing a table into smaller, more manageable parts, based on one or more column values.
    
19. Load the DataFrame into the target Hive table.
    The transformed data is loaded from the DataFrame into the Hive table.

20. Create a directory 'Archive' in the 'fleet-manual' bucket.
    This is where the source file will be moved after it has been loaded into the Hive table.

21. Check if the source file already exists in the 'Archive' directory.
    - If it exists, skip the move operation and print a message.
    - If it doesn't exist, move the source file from the bucket to the 'Archive' directory.
    This step is to avoid overwriting any files in the 'Archive' directory.

22. Print a message to indicate the end of the script.
    This is for monitoring and debugging purposes, to confirm that the script has completed its execution.
"""

# IMPORTS
#############################################################################
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
import io, os, re, sys, time
from botocore.exceptions import NoCredentialsError, ClientError
import boto3
from botocore.client import Config
import pytz


# PARSING ARGUMENTS
############################################################################
ENV=sys.argv[1]


# START SPARK
#############################################################################
spark = (
    SparkSession.builder
    .appName("FLEET_OOS-INC")
    .enableHiveSupport()
    .getOrCreate()
)
print('Spark Version: ' + spark.version)


# SET THE AWS S3 CLIENT
############################################################################
_AWS_ACCESS_KEY_ID = spark.conf.get("spark.hadoop.fs.s3a.access.key")
_AWS_SECRET_ACCESS_KEY = spark.conf.get("spark.hadoop.fs.s3a.secret.key")
_ENDPOINT_URL = spark.conf.get("spark.hadoop.fs.s3a.endpoint")

session = boto3.session.Session()
s3_client = session.client(
    service_name='s3',
    aws_access_key_id=_AWS_ACCESS_KEY_ID,
    aws_secret_access_key=_AWS_SECRET_ACCESS_KEY,
    endpoint_url=_ENDPOINT_URL,
    config=Config(s3={'addressing_style': 'path'})
)
print(f"Boto3 Version: {boto3.__version__}")


# SET THE S3 BUCKET & APPEND THE CURRENT EST DATE 
############################################################################
bucket_name      = 'fleet-manual'                     # S3 BUCKET
source_file      = 'MODEL_NET.dat'                    # INCREMENTAL FILE

archive_bucket_name = 'fleet-manual-archive/FLEET_OOS_DATA_ARCHIVE'

eastern_tz       = pytz.timezone('US/Eastern')        # TIMEZONE AS EST
current_time_et  = datetime.now(eastern_tz)           # CURRENT EST TIMEZONE

# GET THE CURRENT DAY OF THE WEEK (0 is Monday, 1 is Tuesday, ..., 6 is Sunday)
current_day_of_week = current_time_et.weekday()
current_date        = current_time_et.strftime('%Y%m%d') # DATE FORMAT: YYYYMMDD

# IF TODAY IS MONDAY, EXIT THE SCRIPT
############################################################################
if current_day_of_week == 0:
    print(f"Today is Monday ({current_date} EST). Exiting script.")
    sys.exit(0)
else:
    print(f"Today is not Monday ({current_date} EST). Continuing with script.")


destination_file = f"MODEL_NET_{current_date}.txt"    # NEW FILENAME 'MODEL_NET_YYYYMMDD.txt'


# CHECK IF THE DESTINATION FILE ALREADY EXISTS IN THE BUCKET
############################################################################
existing_files = s3.list_objects(Bucket=bucket_name, Prefix=destination_file)

# IF THE FILE EXISTS: SKIP COPY AND EXIT
if 'Contents' in existing_files:
    print(f"File {destination_file} already exists in the bucket, skipping copy.")
    sys.exit(0)

# IF THE FILE DOESN'T EXIST: COPY TO BUCKET WITH DATE APPENDED
response = s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': source_file}, Key=destination_file)
print(f"File {destination_file} copied to bucket {bucket_name}.")


# REMOVE THE SOURCE FILE FROM THE BUCKET 
#s3.delete_object(Bucket=bucket_name, Key=source_file)


# DEFINE THE SCHEMA FOR THE INPUT MODEL_NET FILES
############################################################################
# DEFINING SCHEMA FOR FILE HEADERS
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


# LOAD INCREMENTAL 'MODEL_NET_YYYYMMDD.txt' INTO DATAFRAME
############################################################################
# READ THE CSV FILES FROM S3, APPLY THE SCHEMA, & SET THE DELIMITER
df_historical = (
    spark.read.format('csv')
    .option("sep", "~")
    .schema(df_schema)
    .option("header", "false")
    .load(f"s3a://{bucket_name}/{destination_file}")
)

# ADD A COLUMN, 'Rpt_Dt' TO APPEND DATE AS: 'MODEL_NET_YYYYMMDD.txt' FROM FILEPATH
df_historical = df_historical.withColumn("Rpt_Dt", lit(current_date))
# CAST 'Rpt_Dt' TO DATE TYPE
df_historical = df_historical.withColumn("Rpt_Dt", to_date("Rpt_Dt", "yyyyMMdd"))

# ADD A COLUMN TITLED "SALES_THROUGH_DT" WITH A VALUE OF ONE-CALENDAR-DAY BEFORE "RPT_DT"
df_historical = df_historical.withColumn("Sales_Through_Dt", date_sub("Rpt_Dt", 1))
# CAST 'Sales_Through_Dt' TO DATE TYPE
df_historical = df_historical.withColumn("Sales_Through_Dt", to_date("Sales_Through_Dt", "yyyyMMdd"))

# CAST ALL DATE COLUMNS TO INTEGER TYPE
for month in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]:
    df_historical = df_historical.withColumn(month, col(month).cast(IntegerType()))

# DEFINE HIVE WRITE LOCATION
#############################################################################
TABLE_NAME = "database.fleet_oos_stg"
TABLE_PATH = f"/sync/{ENV.lower()}/FLEET_OOS_STG/Data"


# WRITE THE DATAFRAME TO THE TABLE
#############################################################################
# CHECK IF THERE ARE ALREADY RECORDS WITH THE RPT_DT BEING APPENDED
matching_rpt_dt = spark.read.parquet(TABLE_PATH).select('Rpt_Dt').where(col('Rpt_Dt').isin(df_historical.select('Rpt_Dt').distinct().rdd.flatMap(lambda x: x).collect()))

if matching_rpt_dt.count() > 0:
    print("There is at least one matching rpt_dt in the table, skipping write.")
else:
    print(f"Writing to {TABLE_NAME} started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    # Write the historical dataframe to the table
    df_historical.write.format("parquet").mode("append").save(TABLE_PATH)
    write_success = True
    print(f"Writing to {TABLE_NAME} completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")



# ARCHIVE THE 'MODEL_NET' FILE
#############################################################################
existing_files_archive = s3_client.list_objects(Bucket=archive_bucket_name, Prefix=archive_key_prefix)
existing_keys_archive = [obj['Key'].split('/')[-1] for obj in existing_files_archive.get('Contents', [])]

# IF THE FILE EXISTS IN THE ARCHIVE BUCKET: SKIP ARCHIVING
if destination_file.split('/')[-1] in existing_keys_archive:
    print(f"File {destination_file} already exists in the archive bucket, skipping copy.")
else:
    # COPY THE FILE TO THE ARCHIVE BUCKET WITH A NEW KEY
    archive_key = f"{archive_key_prefix}/{destination_file}"
    response = s3_client.copy_object(Bucket=archive_bucket_name, CopySource={'Bucket': bucket_name, 'Key': destination_file}, Key=archive_key)
    print(f"File {destination_file} copied to archive bucket.")

    # DELETE THE EXISTING FILE FROM THE BUCKET
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=destination_file)
        print(f"File {destination_file} already exists in the bucket, deleted the existing file.")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # Print a message if the destination file does not exist in the bucket
            print(f"File {destination_file} does not exist in the bucket, copying file.")
        else:
            # Raise an exception if any other error occurs
            raise e

# ERROR HANDLING
#############################################################################
spark.stop()
sys.exit(0)
