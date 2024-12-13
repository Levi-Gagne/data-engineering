#!/usr/bin/env python
# coding: utf-8

#############################################################################
# IMPORTS
#############################################################################
from pyspark.sql import SparkSession
#Operational
import sys
from datetime import datetime #to parameterize the date time
from pyspark.sql.functions import to_date, col, substring, struct, concat, date_format, lit
from pyspark.sql import functions as sf
from pyspark.sql.types import *
import time, re, pandas as pd, io, logging
import boto3                          #ECS Import
from botocore.client import Config    #ECS Import

############################################################################
# PARSING ARGUMENTS
############################################################################
ENV=sys.argv[1]

#############################################################################
# START SPARK
#############################################################################
spark = (
    SparkSession.builder
    .appName("sz92q9")
    .enableHiveSupport() # Enable Hive support
    .config("spark.sql.sources.default", "hive") # Set default data source to Hive
    .getOrCreate()
)
print('Spark Version: ' + spark.version)

############################################################################
# DEFINE THE AWS S3 CLIENT
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
# DEFINE A LIST OF THE SEVEN OBJ_ FILE TYPES
############################################################################
file_types = ["OBJ_BAC", "OBJ_REG", "OBJ_ZON", "OBJ_ARE", "OBJ_GMM", "OBJ_NAT", "OBJ_SEC"]

############################################################################
# DEFINE THE S3 BUCKET WHERE THE OBJ_ DATA IS LOCATED
############################################################################
bucket = 'fleet-manual'

############################################################################
# DEFINE THE S3 BUCKET & PREFIXES FOR EACH FILE TYPE
############################################################################
prefixes = {
    'OBJ_BAC': 'OBJ_DATA/OBJ_DATA_Historical/OBJ_BAC',
    'OBJ_REG': 'OBJ_DATA/OBJ_DATA_Historical/OBJ_REG',
    'OBJ_ZON': 'OBJ_DATA/OBJ_DATA_Historical/OBJ_ZON',
    'OBJ_ARE': 'OBJ_DATA/OBJ_DATA_Historical/OBJ_ARE',
    'OBJ_GMM': 'OBJ_DATA/OBJ_DATA_Historical/OBJ_GMM',
    'OBJ_NAT': 'OBJ_DATA/OBJ_DATA_Historical/OBJ_NAT',
    'OBJ_SEC': 'OBJ_DATA/OBJ_DATA_Historical/OBJ_SEC'
}
for key in prefixes:
    print(f"File path: s3://{bucket}/{prefixes[key]}")

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

######################################################################################################
# LOOP OVER EACH FILE TYPE AND APPLY TRANSFORMATIONS BEFORE WRITING TO THE APPLIACABLE TABLE IN HDFS
######################################################################################################
	# DEFINE THE TABLE NAME AND HDFS PATH FOR THIS FILE TYPE
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

	# DEFINE THE SPLIT POSITIONS FOR THIS FILE TYPE
    splits = splits_map[file_type]
	
    # Load the text file into a DataFrame and filter out the header rows
    df = spark.read.text("s3a://{}/{}/*.txt".format(bucket, prefixes[file_type])) \
            .filter(~col("value").startswith("HDR"))
			
	# SPLIT THE VALUE COLUMN INTO SEPERATE COLUMNS
    for i in range(len(splits)):
        start, end = splits[i]
        df = df.withColumn(f"col{i+1}", substring(col("value"), start, end))
		
	# DROP THE VALUE COLUMN FROM THE DATAFRAME
    df = df.drop("value")
	
    # Convert the separate columns to the desired schema
    schema = schemas[file_type]
    for i in range(len(schema.fields)):
        df = df.withColumnRenamed(f"col{i+1}", schema.fields[i].name)
        df = df.withColumn(schema.fields[i].name, col(schema.fields[i].name).cast(schema.fields[i].dataType))
		
	# EXTRACT YEAR, MONTH, & DAY FROM 'Rpt_Dt' & FORMAT THE DATE AS 'YYYYMMDD'
    df = df.withColumn("Rpt_Dt", to_date(col("Rpt_Dt"), "MMddyyyy"))
    df = df.withColumn("Rpt_Dt", date_format(col("Rpt_Dt"), "yyyyMMdd"))
	
	# CAST THE COLUMNS TO THE DESIRED SCHEMA
    for field in schema.fields:
        if field.name == "Biz_Assoc_Id":
            df = df.withColumn(field.name, col(field.name).cast(LongType()))
        elif field.name == "Sell_Src_Cd" or field.name == "Obj_Cnt":
            df = df.withColumn(field.name, col(field.name).cast(IntegerType()))
        elif field.name == "Rpt_Dt":
            df = df.withColumn(field.name, to_date(col(field.name), "yyyyMMdd"))
        else:
            df = df.withColumn(field.name, col(field.name).cast(StringType()))
			
    # PRINT THE RESULTING DATAFRAME & METADATA
    print(f"Dataframe for {file_type}:")
    df.show(5)
    print(f"Row count for {file_type}: {df.count()}")
    print(f"Table name for {file_type}: {table_name}")
    print(f"Parquet path for {file_type}: {table_path}")
	
	# WRITE THE DATAFRAME TO THE APPROPRIATE LOCATION
    try:
        df.write.mode("overwrite").parquet(table_path)
        print(f"{table_name} loaded to {table_path}")
    except Exception as e:
        print(f"Error writing {file_type} data: {str(e)}")

	# ADDITIONAL CODE TO PRINT TABLE NAME & PARQUET PATH
    print(f"Table name for {file_type}: {table_name}")
    print(f"Parquet path for {file_type}: {table_path}")

###########################################################################################################
# STOPE THE SPARK-SESSION
###########################################################################################################
spark.stop()
sys.exit(0)