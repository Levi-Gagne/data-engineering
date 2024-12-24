#!/usr/bin/env python
# coding: utf-8

"""
Script: 
- S3_Email_Notifier.py

This Python script is designed to interact with AWS S3 to verify the presence of specific files,
prepare email notifications based on file availability, and generate a YAML file with email contents.
This script is intended for use with Apache Spark and utilizes environment variables and command-line
arguments to enhance flexibility and ease of configuration for different operational environments.

Requirements:
- Apache Spark
- Python 3
- boto3 library
- PyYAML library

Usage:
Execute this script within a Spark session. Customize the script behavior by setting environment
variables or by passing command-line arguments.

Author:
- Levi Gagne
"""

# Import required modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import boto3
from botocore.client import Config
import os
import sys
import re
import yaml

# Constants and Defaults
DEFAULT_ENV = 'prd'
DEFAULT_BUCKET = 'default_bucket'
DEFAULT_ARCHIVE_BUCKET = 'default_archive_bucket'
DEFAULT_ARCHIVE_FOLDER = 'archive_folder'
DEFAULT_RECIPIENT = 'levi.gagne@outlook.com'

# Configuration via Environment Variables
env = os.getenv('ENVIRONMENT', DEFAULT_ENV)
bucket_name = os.getenv('BUCKET_NAME', DEFAULT_BUCKET)
archive_bucket = os.getenv('ARCHIVE_BUCKET', DEFAULT_ARCHIVE_BUCKET)
archive_folder = os.getenv('ARCHIVE_FOLDER', DEFAULT_ARCHIVE_FOLDER)
email_recipient = os.getenv('EMAIL_RECIPIENT', DEFAULT_RECIPIENT)

# ==================================================
# Initialize Spark Session
# ==================================================
spark = SparkSession.builder.appName("OnTRAC_STG-INC").enableHiveSupport().getOrCreate()
print(f'Spark Version: {spark.version}')

# ==================================================
# Setup the AWS S3 Client
# ==================================================
session = boto3.session.Session()
s3_client = session.client(
    service_name='s3',
    aws_access_key_id=spark.conf.get("spark.hadoop.fs.s3a.access.key"),
    aws_secret_access_key=spark.conf.get("spark.hadoop.fs.s3a.secret.key"),
    endpoint_url=spark.conf.get("spark.hadoop.fs.s3a.endpoint"),
    config=Config(s3={'addressing_style': 'path'})
)
print(f"Boto3 Version: {boto3.__version__}")

# Get Current Date in Different Formats
current_date = datetime.now()
date_format_mmddyyyy = current_date.strftime("%m%d%Y")
date_format_yyyymmdd = current_date.strftime("%Y%m%d")
file_pattern = f"OnTRAC_Out_{date_format_mmddyyyy}_new_{date_format_yyyymmdd}.*\\.txt"

# Search for Specific Files in S3 Bucket
objects = s3_client.list_objects_v2(Bucket=bucket_name)['Contents']
matching_files = [obj['Key'] for obj in objects if re.match(file_pattern, obj['Key'])]

# Email Content Preparation Function
def prepare_email_contents(subject, body, recipient, attachments=None):
    """Prepare email content dictionary, ensuring required fields and formats."""
    if not subject or not body or not recipient:
        raise ValueError("Subject, body, and recipient are required.")
    return {
        "subject": subject,
        "body": body,
        "recipient": recipient,
        "attachments": attachments if attachments else []
    }

# Directory for Temporary YAML File
directory = f"/data/commonScripts/DDA/dsr/{env}/{env}-base/pyspark"

# Validate and Possibly Create Directory for Storing the YAML File
def validate_yml_path(directory):
    """Ensure directory exists; create if not."""
    os.makedirs(directory, exist_ok=True)

# Create YAML File with Email Content
def create_email_yml(directory, email_dict):
    """Write email contents to a YAML file and adjust permissions."""
    file_path = os.path.join(directory, "email_contents.yml")
    with open(file_path, "w") as file:
        yaml.dump(email_dict, file, default_flow_style=False)
    os.chmod(file_path, 0o777)
    print(f"Email content saved to {file_path}")

# Processing Based on File Matching
if not matching_files:
    email_subject = "Ontrac Out Report Production"
    email_body = f"No files matched '{file_pattern}' in '{bucket_name}' or '{archive_bucket}/{archive_folder}', and no data for today ({date_format_yyyymmdd}) is in target table. Please check the source and consider manual intervention."
    email_content = prepare_email_contents(email_subject, email_body, email_recipient)
    validate_yml_path(directory)
    create_email_yml(directory, [email_content])
    print("Email content prepared and saved due to no matching files found.")
    spark.stop()
    sys.exit(0)
else:
    print("Matching files found, no email will be sent.")

# Shutdown Spark session if script completes without sending an email
spark.stop()
