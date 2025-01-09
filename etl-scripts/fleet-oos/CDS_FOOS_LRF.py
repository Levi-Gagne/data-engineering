#!/usr/bin/env python
# coding: utf-8


SCRIPT_NAME = "CDS_FLEET_OUT_OF_STOCK_LRF"

TIDAL_NAME  = "42124-edw-prd-sm-dcfpa-dsr-fleet-out-of-stock-lrf"


# ============================================================================
# Imports
# ============================================================================
# Import standard Python libraries
from pytz import timezone
from datetime import datetime, timedelta
import os, re, sys, time, logging, argparse
# Import PySpark libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, DateType, TimestampType
from pyspark.sql.functions import date_add
from py4j.protocol import Py4JJavaError
from pyspark.sql.utils import AnalysisException, ParseException


# ============================================================================
# Initialize Spark
# ============================================================================
spark = (SparkSession.builder
         .appName(f"{SCRIPT_NAME}")
         .config("spark.driver.memory", "16g")                  # 16GB for the driver
         .config("spark.executor.memory", "16g")                # 16GB per executor
         .config("spark.sql.debug.maxToStringFields", 100)      # Set the limit to 100 fields
         .config("spark.sql.shuffle.partitions", 300)           # Number of partitions for shuffles
         .config("spark.default.parallelism", 300)              # Default number of parallel tasks
         .config("spark.dynamicAllocation.enabled", "true")     # Enable dynamic allocation
         .config("spark.dynamicAllocation.initialExecutors", 2) # Start with two executors
         .config("spark.dynamicAllocation.minExecutors", 1)     # Minimum number of executors
         .config("spark.dynamicAllocation.maxExecutors", 20)    # Maximum number of executors
         .config("spark.sql.session.timeZone", "EST")           # Set SQL Timezone to Eastern Standard Time (EST)
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel('ERROR') # Set log level to ERROR to minimize log output

print(f"Spark Session: {spark.version}")
print(f"Python Version: {sys.version}")


# ============================================================================
# Initialize Variables
# ============================================================================
# Environment Variable
ENV        = sys.argv[1]
ENV        = ENV.lower()

#   -------------------      Source-Tables      -------------------  #
LOOK_TABLE = "marketing360_public.fs_vsna_dim_selling_day_report_lkup"   # Lookup Table: Used to identify which column needs summation in the associated fleet_oos_stg, for any given Rpt_Dt
FOOS_TABLE = "dl_edge_base_dcfpa_171749_base_dcfpa_avista.fleet_oos_stg" # Fleet OOS Staging Table Used to Fetch Data needed to obtain Daily & MTD KPIs

# Assuming you've read your data into DataFrames
df_lookup  = spark.read.table(LOOK_TABLE)
df_foos    = spark.read.table(FOOS_TABLE)

#spark.catalog.refreshTable(LOOK_TABLE)
#spark.catalog.refreshTable(FOOS_TABLE)

# Create temporary tables
df_lookup.createOrReplaceTempView("temp_lookup")
df_foos.createOrReplaceTempView("temp_foos")
#   -------------------      Source-Tables      -------------------  #


#   -------------------      Target-Table       -------------------  #
# Target-Table
TARGET_TABLE_NAME = "sales_mktg.cds_fleet_out_of_stock_lrf"
TARGET_TABLE_PATH = f"/sync/{ENV}_42124_edw_b/EDW/SALES_MKTG/RZ/CDS_FLEET_OUT_OF_STOCK_LRF/Processing"

# Target-Target-Schema
TARGET_TABLE_SCHEMA = [
    "Rpt_Dt",
    "Sales_Thru_Dt",
    "Src_Sys_Id",
    "Day_Sales_Cnt", 
    "Mth_To_Dt_Sales_Cnt",
    "Rpt_Mth_Nbr", 
    "Rpt_Mth_Abbr_Txt",
    "Src_Sys_Uniq_Prim_Key_Val", 
    "Src_Sys_Uniq_Prim_Key_Col_Val",
    "Meas_Cnt", 
    "Dw_Anom_Flg", 
    "Dw_Mod_Ts", 
    "Dw_Job_Id", 
    "Src_Sys_Iud_Cd",
]

# Plumbing-Columns
SRC_SYS_ID     = F.lit("315")
MEAS_CNT       = F.lit(1)
DW_ANOM_FLG    = F.lit("N")
CURRENT_TS     = F.current_timestamp()
DW_JOB_ID      = F.lit(f"{TIDAL_NAME}")
SRC_SYS_IUD_CD = F.lit("I")
SRC_SYS_UNIQ_PRIM_KEY_VAL     = F.concat(F.col("Rpt_Dt"), F.lit("|"), F.col("Sales_Thru_Dt"), F.lit("|"), SRC_SYS_ID)
SRC_SYS_UNIQ_PRIM_KEY_COL_VAL = F.lit("Rpt_Dt|Sales_Thru_Dt|Src_Sys_Id")
#   -------------------      Target-Table       -------------------  #


# Month List: To Reduce Redundancy
MONTH_LIST = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]


# ============================================================================
# Data Ingestion
# ============================================================================
# *Now you can query these temp tables*
# Fetch sales date and corresponding start of the month date
lookup_data = spark.sql("""
    SELECT 
        salesthroughdate AS sell_dt, 
        currentmonthmthstart AS curr_mth_start_dt
    FROM temp_lookup
""")

# Fetch multiple fields from 'fleet_oos_stg' and alias as 'fleet_OOS'
fleet_oos = spark.sql("""
    SELECT 
        rpt_dt, 
        sales_through_dt, 
        jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, `dec`
    FROM temp_foos AS fleet_OOS
""")


# ============================================================================
# Calculating MTD Sales
# ============================================================================
# Aggregate Monthly Sales by Report Date
agg_exprs = [F.sum(month).alias(month) for month in MONTH_LIST]
agg_fleet_oos = fleet_oos.groupBy("rpt_dt").agg(*agg_exprs)


# Create a Window Specification ordered by 'Rpt_Dt'
windowSpec = Window.orderBy("rpt_dt")

# Perform an inner join based on the adjusted 'Rpt_Dt' or 'Sales_Through_Dt'
fleet_oos_with_lookup = agg_fleet_oos.join(
    lookup_data,
    (date_add(agg_fleet_oos.rpt_dt, -1) == lookup_data.sell_dt),
    'inner'
).select("rpt_dt", "curr_mth_start_dt", *MONTH_LIST)


agg_fleet_oos_with_mtd = fleet_oos_with_lookup.withColumn(
    "mtd_sales",
    F.expr("""
        CASE 
            WHEN MONTH(curr_mth_start_dt) = 1 AND rpt_dt = curr_mth_start_dt THEN dec
            WHEN MONTH(curr_mth_start_dt) = 2 AND rpt_dt = curr_mth_start_dt THEN jan
            WHEN MONTH(curr_mth_start_dt) = 3 AND rpt_dt = curr_mth_start_dt THEN feb
            WHEN MONTH(curr_mth_start_dt) = 4 AND rpt_dt = curr_mth_start_dt THEN mar
            WHEN MONTH(curr_mth_start_dt) = 5 AND rpt_dt = curr_mth_start_dt THEN apr
            WHEN MONTH(curr_mth_start_dt) = 6 AND rpt_dt = curr_mth_start_dt THEN may
            WHEN MONTH(curr_mth_start_dt) = 7 AND rpt_dt = curr_mth_start_dt THEN jun
            WHEN MONTH(curr_mth_start_dt) = 8 AND rpt_dt = curr_mth_start_dt THEN jul
            WHEN MONTH(curr_mth_start_dt) = 9 AND rpt_dt = curr_mth_start_dt THEN aug
            WHEN MONTH(curr_mth_start_dt) = 10 AND rpt_dt = curr_mth_start_dt THEN sep
            WHEN MONTH(curr_mth_start_dt) = 11 AND rpt_dt = curr_mth_start_dt THEN oct
            WHEN MONTH(curr_mth_start_dt) = 12 AND rpt_dt = curr_mth_start_dt THEN nov
            ELSE
                CASE
                    WHEN MONTH(curr_mth_start_dt) = 1 THEN jan
                    WHEN MONTH(curr_mth_start_dt) = 2 THEN feb
                    WHEN MONTH(curr_mth_start_dt) = 3 THEN mar
                    WHEN MONTH(curr_mth_start_dt) = 4 THEN apr
                    WHEN MONTH(curr_mth_start_dt) = 5 THEN may
                    WHEN MONTH(curr_mth_start_dt) = 6 THEN jun
                    WHEN MONTH(curr_mth_start_dt) = 7 THEN jul
                    WHEN MONTH(curr_mth_start_dt) = 8 THEN aug
                    WHEN MONTH(curr_mth_start_dt) = 9 THEN sep
                    WHEN MONTH(curr_mth_start_dt) = 10 THEN oct
                    WHEN MONTH(curr_mth_start_dt) = 11 THEN nov
                    WHEN MONTH(curr_mth_start_dt) = 12 THEN dec
                END
        END
    """)
)

# ============================================================================
# Calculating Daily Sales From MTD Sales
# ============================================================================
# Select only the columns you need: "rpt_dt", "curr_mth_start_dt", and "mtd_sales"
final_df        = agg_fleet_oos_with_mtd.select("rpt_dt", "curr_mth_start_dt", "mtd_sales")


# Sort the DataFrame by 'rpt_dt' in descending order
sorted_final_df = final_df.orderBy(F.desc("rpt_dt"))


# Windowing & Lag Operations
sorted_final_df = (sorted_final_df
                   .withColumn("prev_day", F.date_add("rpt_dt", -1))
                   .withColumn("prev_day_mtd_sales", F.lag("mtd_sales").over(windowSpec)))


# Prepare the final DataFrame 'df_SQL' by selecting and aliasing necessary columns.
df_SQL = sorted_final_df.select(
    F.col("rpt_dt").alias("Rpt_Dt"),
    F.col("prev_day").alias("Sales_Thru_Dt"),
    F.col("curr_mth_start_dt"), 
    F.when(
        (F.col("prev_day") == F.col("curr_mth_start_dt")) & (F.month("curr_mth_start_dt") == 1), 0
    ).when(
        F.col("prev_day") == F.col("curr_mth_start_dt"), F.col("mtd_sales")
    ).when(
        F.col("prev_day").isNull(), F.col("mtd_sales")
    ).otherwise(
        F.col("mtd_sales") - F.col("prev_day_mtd_sales")
    ).alias("Day_Sales_Cnt"),
    F.col("mtd_sales").alias("Mth_To_Dt_Sales_Cnt")
).orderBy(
    F.desc("Rpt_Dt")
)


# Drop duplicates and pick the first value for both 'Day_Sales_Cnt' & 'Mth_To_Dt_Sales_Cnt'
df_MERGED = df_SQL.groupBy("Rpt_Dt").agg(
    F.first("Day_Sales_Cnt", ignorenulls=True).alias("Day_Sales_Cnt"),
    F.first("Mth_To_Dt_Sales_Cnt", ignorenulls=True).alias("Mth_To_Dt_Sales_Cnt")
)


# ============================================================================
# Transformations
# ============================================================================
df_FINAL = (
    df_MERGED
    # TRANSFORM SOURCE DATA
    .withColumn('Sales_Thru_Dt', F.date_add('Rpt_Dt', -1).cast("date"))
    .withColumn('Day_Sales_Cnt', F.col('Day_Sales_Cnt').cast('int'))
    .withColumn('Mth_To_Dt_Sales_Cnt', F.col('Mth_To_Dt_Sales_Cnt').cast('int'))
    .withColumn('Rpt_Mth_Nbr', F.date_format(F.col('Rpt_Dt'), 'MM').cast('int'))
    .withColumn('Rpt_Mth_Abbr_Txt', F.date_format(F.col('Rpt_Dt'), 'MMM'))
    # PLUMBING COLUMNS
    .withColumn("Src_Sys_Id", SRC_SYS_ID)
    .withColumn("Src_Sys_Uniq_Prim_Key_Val", SRC_SYS_UNIQ_PRIM_KEY_VAL)
    .withColumn("Src_Sys_Uniq_Prim_Key_Col_Val", SRC_SYS_UNIQ_PRIM_KEY_COL_VAL)
    .withColumn("Meas_Cnt", MEAS_CNT)
    .withColumn("Dw_Anom_Flg", DW_ANOM_FLG)
    .withColumn("Dw_Mod_Ts", CURRENT_TS)
    .withColumn("Dw_Job_Id", DW_JOB_ID)
    .withColumn("Src_Sys_Iud_Cd", SRC_SYS_IUD_CD)
    .select(*TARGET_TABLE_SCHEMA)  # Reorder the DataFrame columns based on 'TARGET_TABLE_SCHEMA'
)

COUNT_df_FINAL = df_FINAL.count()
print(f"Showing 'df_FINAL', which has a row count of {COUNT_df_FINAL}, below")

df_FINAL.show(5)


# ============================================================================
# Overwrite Hive
# ============================================================================
print(f"Writing '{COUNT_df_FINAL}' rows from the final dataframe to '{TARGET_TABLE_NAME}'")

df_FINAL.write.format('parquet').mode('overwrite').saveAsTable(TARGET_TABLE_NAME, path=TARGET_TABLE_PATH)

print(f"Finished writing to '{TARGET_TABLE_NAME}' & EXITING '{SCRIPT_NAME}'")


# ============================================================================
# Stop Spark-Session
# ============================================================================
spark.stop()
sys.exit(0)
