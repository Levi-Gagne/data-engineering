from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, regexp_extract, input_file_name
from datetime import datetime
import pytz

def log_info(message: str, tag: str = "[INFO]") -> None:
    print(f"{tag} {message}")

def get_current_date():
    """Returns the current date in YYYYMMDD format."""
    chicago_tz = pytz.timezone("America/Chicago")
    return datetime.now(chicago_tz).strftime("%Y%m%d")

def load_today_data(spark, path, schema, today_str):
    """
    Loads only today's files by using a wildcard for state names.

    Args:
        spark (SparkSession): Spark session.
        path (str): Base directory path.
        schema (StructType): Schema for reading CSV.
        today_str (str): Today's date in YYYYMMDD format.

    Returns:
        DataFrame: Transformed DataFrame with 'state' and 'file_path' columns.
    """
    # Construct file pattern to match only today's files with wildcard for state names
    file_pattern = f"{path}/*_grocery_supply_{today_str}.csv"

    # Load only matching files
    df = (spark.read.format("csv")
          .option("header", "true")
          .schema(schema)
          .load(file_pattern))  # Directly filter files at load time

    log_info(f"Loaded data from {file_pattern}", "[LOAD]")

    # Add file path and extract state
    df = df.withColumn("file_path", input_file_name()) \
           .withColumn("state", regexp_extract(input_file_name(), ".*/([^/_]+)_grocery_supply_\\d{8}\\.csv", 1))

    # Debugging: Print schema and sample records
    df.printSchema()
    df.show(5, truncate=False)

    return df

def write_data(df, table_name):
    """Writes the DataFrame to the target Delta table."""
    df.write.format("delta").mode("append").saveAsTable(table_name)
    log_info(f"Data written to {table_name}.", "[WRITE]")

def main():
    spark = SparkSession.builder.appName("Grocery Supply Bronze Ingestion").getOrCreate()

    # Get current date
    today_str = get_current_date()

    # Define path with wildcard for state names
    volume_path = f"/Volumes/dq_dev/lmg_sandbox/grocery_data/grocery_state_supply"

    # Define target table
    target_table = "dq_dev.lmg_sandbox.grocery_supply_stg"

    # Define schema for CSV files
    csv_schema = StructType([
        StructField("supplier_name", StringType(), True),
        StructField("produce_type", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("today_date", StringType(), True)
    ])

    # Define target table schema (adds `state` and `file_path`)
    target_schema = StructType(csv_schema.fields + [
        StructField("state", StringType(), True),
        StructField("file_path", StringType(), True)
    ])

    # Load and process today's data
    df = load_today_data(spark, volume_path, csv_schema, today_str)

    # Write to Delta table
    write_data(df, target_table)

    spark.stop()

if __name__ == "__main__":
    main()
