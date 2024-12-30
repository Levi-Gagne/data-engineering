#!/usr/bin/env python
# coding: utf-8

# IMPORTS
#############################################################################
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.window import Window
from datetime import datetime
import boto3
from botocore.client import Config
import sys

# PARSING ARGUMENTS
############################################################################
ENV = sys.argv[1]

# Check if the environment variable is set to one of the expected values
#if ENV not in ["DEV", "TST", "CRT", "PRD"]:
#   raise Exception('Environment Variable NOT passed properly')


# START SPARK
#############################################################################
spark = (
SparkSession.builder
.appName("OnTRAC_STG-HIS") \
.enableHiveSupport() \
.getOrCreate()
)
print('Spark Version: ' + spark.version)


# DEFINE THE AWS S3 Client
#############################################################################
session = boto3.session.Session()
s3_client = session.client(
    service_name='s3',
    aws_access_key_id=spark.conf.get("spark.hadoop.fs.s3a.access.key"),
    aws_secret_access_key=spark.conf.get("spark.hadoop.fs.s3a.secret.key"),
    endpoint_url=spark.conf.get("spark.hadoop.fs.s3a.endpoint"),
    config=Config(s3={'addressing_style': 'path'})
)
print("Boto3 Version: {}".format(boto3.__version__))


# SPECIFY ARCHIVE LOCATION IN S3
#############################################################################
bucket_name   = "fleet-manual-archive"                  
folder_name   = "OnTRAC_ARCHIVE"


# DEFINE DATAFRAME SCHEMA
#############################################################################
df_schema = StructType([
    StructField("VIN", StringType()),
    StructField("outDate", StringType()),
    StructField("outMile", StringType()),
    StructField("todayDate", StringType()),
    StructField("outType", StringType()),
    StructField("daysInService", StringType()),
    StructField("loanGreaterThan60", StringType())
])

# LOAD THE HISTORICAL 'OnTRAC_OUT.txt' FILES INTO DATAFRAME
#############################################################################
try:
    df = spark.read.format('csv') \
        .option("sep", "|") \
        .schema(df_schema) \
        .option("header", "false") \
        .load(f"s3a://{bucket_name}/{folder_name}/OnTRAC_Out_*.txt")

    df.show(5)
    df.printSchema()

    # CAST COLUMNS TO THE DESIRED DATA TYPE
	#############################################################################
    df = df.withColumn("outDate", to_date(col("outDate"), "MM/dd/yyyy"))
    df = df.withColumn("todayDate", to_date(col("todayDate"), "MM/dd/yyyy"))
    df = df.withColumn("outMile", col("outMile").cast(IntegerType()))
    df = df.withColumn("daysInService", col("daysInService").cast(IntegerType()))

    df.show(5)
    df.printSchema()
except Exception as e:
    print("Error occurred while loading data and casting columns: ", e)


# TABLE NAME: Static
TABLE_NAME = "dl_edge_base_dcfpa_171749_base_dcfpa_avista.ret_crtsy_trp_pgm_stg"
# TABLE PATH IS DYNAMIC, BASED ON THE ENVIRONMENT VARIABLE
TABLE_PATH = "/sync/tst_42124_edw_hi_b/DL/EDGE_BASE/DCFPA-171749/DCFPA-AVISTA/RET_CRTSY_TRP_PGM_STG/Data"

# WRITE DATA TO TABLE IN HDFS
#############################################################################
try:
    print("Writing to {} started: {}".format(TABLE_NAME, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    df.write.format('parquet').mode('overwrite').saveAsTable(TABLE_NAME, path=TABLE_PATH)
    print("Writing to {} completed: {}".format(TABLE_NAME, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
except Exception as e:
    print("Failed to write dataframe to HDFS table {}! Error: ".format(TABLE_NAME), e)

# CONFIRM SUCCESS
#############################################################################
try:
    print("Confirming write to HDFS table {}...".format(TABLE_NAME))
    df_test = spark.read.parquet(TABLE_PATH)

    if df.subtract(df_test).count() == 0 and df_test.subtract(df).count() == 0:
        print("Dataframe written to HDFS table {} successfully!".format(TABLE_NAME))
    else:
        print("Data in original DataFrame and HDFS table {} do not match!".format(TABLE_NAME))
except Exception as e:
    print("Failed to read from HDFS table {}! Error: ".format(TABLE_NAME), e)

# STOP SPARK SESSION & EXIT SCRIPT
#############################################################################
spark.stop()
sys.exit(0)