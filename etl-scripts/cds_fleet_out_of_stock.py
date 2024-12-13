#!/usr/bin/env python
# coding: utf-8


"""
Script Documentation: Fleet Out Of Stock Data Processing
###############################################################

Overview:
This Python script performs data processing related to fleet out-of-stock records in a corporate environment. It reads data from source tables 'marketing360_public.fs_vsna_dim_selling_day_report_lkup' and 'dl_edge_base_dcfpa_171749_base_dcfpa_avista.fleet_oos_stg', then processes and writes the results to the user-defined target table, typically in the format of 'sales_mktg.{script_name}'. The script utilizes PySpark for efficient large-scale data handling and includes functions to initialize Spark sessions, create temporary views, union dataframes, create additional columns, and write data to tables.

Environment:
- Language: Python (with standard libraries)
- External Libraries: PySpark
- Required Systems: Spark, Hive (for table operations)

Core Functionality:
- Reads From:
  - 'marketing360_public.fs_vsna_dim_selling_day_report_lkup'
  - 'dl_edge_base_dcfpa_171749_base_dcfpa_avista.fleet_oos_stg'
- Writes To: User-defined target table, typically 'sales_mktg.{script_name}'
- Operations: Initializes Spark sessions, creates temporary views, executes SQL queries, unions DataFrames, generates additional columns, and writes data to the target table.

Script Functions:
1. 'initialize_spark_session(app_name="SZ92Q9")': Initializes a Spark session with custom configurations.
2. 'create_temp_views(spark, look_table, foos_table)': Creates temporary views from given source-table names.
3. 'union_dataframes(spark, sql_queries)': Executes a list of SQL queries and unions the resulting DataFrames.
4. 'create_plumbing_columns(detroit_tz, tidal_name)': Generates plumbing columns for the data processing.
5. 'write_data_to_table(df, target_table_name, target_table_path, current_timestamp, spark_session, mode="overwrite", colors=None)': Writes the processed DataFrame to the specified table.
6. 'main(...)': The main function orchestrates the data processing.

Usage:
- Execution: Run on a Spark-enabled environment with access to the necessary tables and storage paths.
- Modifications: Adapt parameters and queries as needed for the specific environment and use-case.

Key Variables and Queries:
- 'daily_historical', 'daily_incremental', 'mtd_historical', 'mtd_incremental': SQL queries for data retrieval.
- 'target_schema': Defines the schema of the target table where data will be written.

Logging and Error Handling:
- Includes timestamped print statements for progress tracking and diagnostics.
- Exception handling to catch and log errors during processing stages.

Security and Compliance:
- Adhere to corporate data security and privacy policies.
- Regularly update access permissions and audit logs.

Conclusion:
This script is a robust solution for processing and managing fleet out-of-stock data, with specific read and write operations clearly defined. It's efficient for handling large datasets and provides clear oversight of data processing and writing stages. Regular reviews and updates are recommended to keep the documentation aligned with any script changes.
"""


# PYTHON STANDARD LIBRARY
import sys, time, pytz, inspect
from datetime import datetime, timedelta
from typing import List
# THIRD-PARTY IMPORTS
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col


def initialize_spark_session(script_name):
    """
    Initializes and returns a SparkSession with custom configurations.
    """
    spark = (SparkSession.builder
             .appName(script_name)
             .config("spark.driver.memory", "16g")
             .config("spark.executor.memory", "16g")
             .config("spark.sql.debug.maxToStringFields", 100)
             .config("spark.sql.shuffle.partitions", 300)
             .config("spark.default.parallelism", 300)
             .config("spark.sql.caseSensitive", "false")
             .config("spark.dynamicAllocation.enabled", "true")
             .config("spark.dynamicAllocation.initialExecutors", 2)
             .config("spark.dynamicAllocation.minExecutors", 1)
             .config("spark.dynamicAllocation.maxExecutors", 20)
             .config("spark.sql.session.timeZone", "EST")
             .enableHiveSupport()
             .getOrCreate())

    print(f"\033[38;2;0;153;204mSpark Version:\033[0m {spark.version}", f"\033[38;2;0;255;128mPython Version:\033[0m {sys.version.split()[0]}")
    spark.sparkContext.setLogLevel('ERROR')

    return spark


def info(df):
    """
    Prints the count, schema, and first few rows of a given DataFrame.
    Automatically determines the DataFrame name using introspection.

    :param df: The DataFrame to process.
    """
    frame   = inspect.currentframe() # Introspection to find the DataFrame name
    df_name = ""
    try:
        df_name = [var_name for var_name, var_val in frame.f_back.f_locals.items() if var_val is df][0]
    finally:
        del frame  # Avoid reference cycles

    count_df = df.count()
    print(f"\033[38;2;255;165;0m'{df_name}'\033[0m has \033[38;2;255;165;0m'{count_df}'\033[0m rows")
    df.printSchema()
    df.show(5)


def create_temp_views(spark, look_table, foos_table):
    """
    Creates temporary views from the given source-table names.
    """
    df_lookup = spark.read.table(look_table)
    df_foos   = spark.read.table(foos_table)

    #spark.catalog.refreshTable(look_table) # Optional 'look_table' Refresh
    #spark.catalog.refreshTable(foos_table) # Optional 'foos_table' Refresh

    df_lookup.createOrReplaceTempView("temp_lookup") # Create temporary table: 'temp_lookup'
    df_foos.createOrReplaceTempView("temp_foos")     # Create temporary table: 'temp_foos'



def create_plumbing_columns(detroit_tz, tidal_name):
    """
    Generates plumbing columns.
    """
    SRC_SYS_ID     = F.lit("171749")
    MEAS_CNT       = F.lit(1)
    DW_ANOM_FLG    = F.lit("N")
    CURRENT_TS     = F.current_timestamp().cast('timestamp').alias('timestamp').withColumn('Detroit_Time', F.from_utc_timestamp('timestamp', detroit_tz))
    DW_JOB_ID      = F.lit(f"{tidal_name}")
    SRC_SYS_IUD_CD = F.lit("I")
    SRC_SYS_UNIQ_PRIM_KEY_VAL     = F.concat(F.col("Rpt_Dt"), F.lit("|"), F.col("Sales_Thru_Dt"), F.lit("|"), F.col("Sales_Mth_Cat_Nbr"), F.lit("|"), SRC_SYS_ID)
    SRC_SYS_UNIQ_PRIM_KEY_COL_VAL = F.lit("Rpt_Dt|Sales_Thru_Dt|Sales_Mth_Cat_Nbr|Src_Sys_Id")

    return SRC_SYS_ID, MEAS_CNT, DW_ANOM_FLG, CURRENT_TS, DW_JOB_ID, SRC_SYS_IUD_CD, SRC_SYS_UNIQ_PRIM_KEY_VAL, SRC_SYS_UNIQ_PRIM_KEY_COL_VAL


daily_historical = """

     -------  Fleet Out Of Stock CDS  -------
        -----   Daily Historical    -----
        
    --Daily Fleet Sales

    -- Fleet OOS
    with fleet_oos_range as (
        select distinct prior_yr_start_dt
    from marketing360_public.fs_gbl_sales_cal_day_rpt_lkp 
    where year(curr_yr_start_dt) = 2022
    and iso_ctry_cd = 'US'
    ),


    rtl_sales_thru as (
    select l.sales_thru_dt,
    l.curr_mth_start_dt,
    month(l.curr_mth_start_dt) as rpt_mth_nbr,
    date_format(l.curr_mth_start_dt, 'MMM') as rpt_mth_abbr_txt,
    l.curr_yr_start_dt,
    year(l.curr_yr_start_dt) as curr_sales_year,
    l.prior_yr_start_dt,
    year(l.prior_yr_start_dt) as prior_yr
    from temp_lookup l, fleet_oos_range f 
    where l.sales_thru_dt <= date_sub(to_date(current_date), 2)
    and l.sales_thru_dt >= f.prior_yr_start_dt
    and l.iso_ctry_cd = 'US'
    order by l.sales_thru_dt
    ), --this query grabs the rtl_sales_thru date    



    pre_day as (
    Select
    fd.rpt_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt,
    Sum(Case
    when fd.sales_through_dt = date_sub(to_date(rtl_sales_thru.curr_yr_start_dt), 1) then 0 
    when fd.sales_through_dt = date_sub(to_date(rtl_sales_thru.prior_yr_start_dt), 1) then 0
    Else fd.jan
    END) as Jan_MTD_Sales,
    sum(fd.feb) as Feb_MTD_Sales,
    sum(fd.mar) as Mar_MTD_Sales,
    sum(fd.apr) as Apr_MTD_Sales,
    sum(fd.may) as May_MTD_Sales,
    sum(fd.jun) as Jun_MTD_Sales,
    sum(fd.jul) as Jul_MTD_Sales,
    sum(fd.aug) as Aug_MTD_Sales,
    sum(fd.sep) as Sep_MTD_Sales,
    sum(fd.oct) as Oct_MTD_Sales,
    sum(fd.nov) as Nov_MTD_Sales,
    sum(fd.`dec`) as Dec_MTD_Sales
    from temp_foos fd, rtl_sales_thru
    Where fd.sales_through_dt = date_sub(to_date(rtl_sales_thru.sales_thru_dt), 1)
    group by fd.rpt_dt, rtl_sales_thru.sales_thru_dt, rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt

    ),

    curr_day as (
    Select
    fd.rpt_dt, 
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt,
    fd.rpt_dt as daily_sales_rpt_dt,
    sum(fd.jan) as Jan_MTD_Sales,
    sum(fd.feb) as Feb_MTD_Sales,
    sum(fd.mar) as Mar_MTD_Sales,
    sum(fd.apr) as Apr_MTD_Sales,
    sum(fd.may) as May_MTD_Sales,
    sum(fd.jun) as Jun_MTD_Sales,
    sum(fd.jul) as Jul_MTD_Sales,
    sum(fd.aug) as Aug_MTD_Sales,
    sum(fd.sep) as Sep_MTD_Sales,
    sum(fd.oct) as Oct_MTD_Sales,
    sum(fd.nov) as Nov_MTD_Sales,
    sum(fd.`dec`) as Dec_MTD_Sales
    from temp_foos fd, rtl_sales_thru
    Where fd.sales_through_dt = to_date(rtl_sales_thru.sales_thru_dt)
    group by fd.rpt_dt, rtl_sales_thru.sales_thru_dt, rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt
    )

    --January Month
    Select
    'Jan' as sales_mth_cat,
    1 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Jan_MTD_Sales -pd.Jan_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 1
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION

    --February Month
    Select
    'Feb' as sales_mth_cat,
    2 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Feb_MTD_Sales -pd.Feb_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 2
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION

    --March Month
    Select
    'Mar' as sales_mth_cat,
    3 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Mar_MTD_Sales -pd.Mar_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 3
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION

    --Aoril Month
    Select
    'Apr' as sales_mth_cat,
    4 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Apr_MTD_Sales -pd.Apr_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 4
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION


    --May Month
    Select
    'May' as sales_mth_cat,
    5 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.May_MTD_Sales -pd.May_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 5
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION


    --June Month
    Select
    'Jun' as sales_mth_cat,
    6 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Jun_MTD_Sales -pd.Jun_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 6
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION


    --July Month
    Select
    'Jul' as sales_mth_cat,
    7 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Jul_MTD_Sales -pd.Jul_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 7
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION


    --August Month
    Select
    'Aug' as sales_mth_cat,
    8 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Aug_MTD_Sales -pd.Aug_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 8
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION


    --September Month
    Select
    'Sep' as sales_mth_cat,
    9 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Sep_MTD_Sales -pd.Sep_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 9
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION


    --October Month
    Select
    'Oct' as sales_mth_cat,
    10 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Oct_MTD_Sales -pd.Oct_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 10
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION


    --November Month
    Select
    'Nov' as sales_mth_cat,
    11 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Nov_MTD_Sales -pd.Nov_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 11
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION


    --December Month
    Select
    'Dec' as sales_mth_cat,
    12 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Dec_MTD_Sales -pd.Dec_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 12
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

        
        -----   Daily Historical    -----
     -------  Fleet Out Of Stock CDS  -------
     
"""


daily_incremental = """

     -------  Fleet Out Of Stock CDS  -------
        -----   Daily Incremental    -----
        
        --Daily Fleet Sales

    -- Fleet OOS
    with day_rtl_sales_thru as (
    select l.sales_thru_dt,
    l.curr_mth_start_dt,
    month(l.curr_mth_start_dt) as rpt_mth_nbr,
    date_format(l.curr_mth_start_dt, 'MMM') as rpt_mth_abbr_txt,
    l.curr_yr_start_dt,
    year(l.curr_yr_start_dt) as curr_sales_year,
    l.prior_yr_start_dt,
    year(l.prior_yr_start_dt) as prior_yr
    from temp_lookup l
    where sales_thru_dt = date_sub(to_date(current_date), 1)
    and iso_ctry_cd = 'US'), --This query provides sales thru dates


    pre_day as (
    Select
    fd.rpt_dt,
    day_rtl_sales_thru.sales_thru_dt,
    day_rtl_sales_thru.rpt_mth_nbr,
    day_rtl_sales_thru.rpt_mth_abbr_txt,
    Sum(Case
    when fd.sales_through_dt = date_sub(to_date(day_rtl_sales_thru.curr_yr_start_dt), 1) then 0 
    when fd.sales_through_dt = date_sub(to_date(day_rtl_sales_thru.prior_yr_start_dt), 1) then 0
    Else fd.jan
    END) as Jan_MTD_Sales,
    sum(fd.feb) as Feb_MTD_Sales,
    sum(fd.mar) as Mar_MTD_Sales,
    sum(fd.apr) as Apr_MTD_Sales,
    sum(fd.may) as May_MTD_Sales,
    sum(fd.jun) as Jun_MTD_Sales,
    sum(fd.jul) as Jul_MTD_Sales,
    sum(fd.aug) as Aug_MTD_Sales,
    sum(fd.sep) as Sep_MTD_Sales,
    sum(fd.oct) as Oct_MTD_Sales,
    sum(fd.nov) as Nov_MTD_Sales,
    sum(fd.`dec`) as Dec_MTD_Sales
    from temp_foos fd, day_rtl_sales_thru
    Where fd.sales_through_dt = date_sub(to_date(day_rtl_sales_thru.sales_thru_dt), 1)
    group by fd.rpt_dt, day_rtl_sales_thru.sales_thru_dt, day_rtl_sales_thru.rpt_mth_nbr,
    day_rtl_sales_thru.rpt_mth_abbr_txt

    ),

    curr_day as (
    Select
    fd.rpt_dt, 
    day_rtl_sales_thru.sales_thru_dt,
    day_rtl_sales_thru.rpt_mth_nbr,
    day_rtl_sales_thru.rpt_mth_abbr_txt,
    fd.rpt_dt as daily_sales_rpt_dt,
    sum(fd.jan) as Jan_MTD_Sales,
    sum(fd.feb) as Feb_MTD_Sales,
    sum(fd.mar) as Mar_MTD_Sales,
    sum(fd.apr) as Apr_MTD_Sales,
    sum(fd.may) as May_MTD_Sales,
    sum(fd.jun) as Jun_MTD_Sales,
    sum(fd.jul) as Jul_MTD_Sales,
    sum(fd.aug) as Aug_MTD_Sales,
    sum(fd.sep) as Sep_MTD_Sales,
    sum(fd.oct) as Oct_MTD_Sales,
    sum(fd.nov) as Nov_MTD_Sales,
    sum(fd.`dec`) as Dec_MTD_Sales
    from temp_foos fd, day_rtl_sales_thru
    Where fd.sales_through_dt = to_date(day_rtl_sales_thru.sales_thru_dt)
    group by fd.rpt_dt, day_rtl_sales_thru.sales_thru_dt, day_rtl_sales_thru.rpt_mth_nbr,
    day_rtl_sales_thru.rpt_mth_abbr_txt
    )

    --January Month
    Select
    'Jan' as sales_mth_cat,
    1 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Jan_MTD_Sales -pd.Jan_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 1
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION

    --February Month
    Select
    'Feb' as sales_mth_cat,
    2 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Feb_MTD_Sales -pd.Feb_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 2
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION

    --March Month
    Select
    'Mar' as sales_mth_cat,
    3 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Mar_MTD_Sales -pd.Mar_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 3
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION

    --Aoril Month
    Select
    'Apr' as sales_mth_cat,
    4 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Apr_MTD_Sales -pd.Apr_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 4
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION


    --May Month
    Select
    'May' as sales_mth_cat,
    5 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.May_MTD_Sales -pd.May_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 5
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION


    --June Month
    Select
    'Jun' as sales_mth_cat,
    6 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Jun_MTD_Sales -pd.Jun_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 6
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION


    --July Month
    Select
    'Jul' as sales_mth_cat,
    7 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Jul_MTD_Sales -pd.Jul_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 7
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION


    --August Month
    Select
    'Aug' as sales_mth_cat,
    8 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Aug_MTD_Sales -pd.Aug_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 8
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION


    --September Month
    Select
    'Sep' as sales_mth_cat,
    9 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Sep_MTD_Sales -pd.Sep_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 9
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION


    --October Month
    Select
    'Oct' as sales_mth_cat,
    10 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Oct_MTD_Sales -pd.Oct_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 10
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION


    --November Month
    Select
    'Nov' as sales_mth_cat,
    11 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Nov_MTD_Sales -pd.Nov_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 11
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt

    UNION


    --December Month
    Select
    'Dec' as sales_mth_cat,
    12 as sales_mth_cat_nbr,
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt,
    sum(f.Dec_MTD_Sales -pd.Dec_MTD_Sales) as Daily_Sales
    from curr_day f 
    left join pre_day pd
    ON (
    (date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
    OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
    )
    Where f.rpt_mth_nbr = 12
    GROUP BY
    f.rpt_dt,
    f.sales_thru_dt,
    f.rpt_mth_nbr,
    f.rpt_mth_abbr_txt


        -----   Daily Incremental    -----
     -------  Fleet Out Of Stock CDS  -------

"""


mtd_historical = """

     -------  Fleet Out Of Stock CDS  -------
          -----   MTD Historical    -----
        
    -- MTD Historical Fleet OOS Sales

    -- Fleet OOS
    With fleet_oos_range as (select sales_thru_dt, prior_yr_start_dt
    from temp_lookup 
    where sales_thru_dt = date_sub(to_date(current_date), 2)
    and iso_ctry_cd = 'US'),--This query provides the range of dates to be used

    rtl_sales_thru as (
    select r.sales_thru_dt,
    r.curr_mth_start_dt,
    month(curr_mth_start_dt) as rpt_mth_nbr,
    date_format(curr_mth_start_dt, 'MMM') as rpt_mth_abbr_txt
    from temp_lookup r, fleet_oos_range f 
    where r.sales_thru_dt <= f.sales_thru_dt
    and r.sales_thru_dt >= f.prior_yr_start_dt
    and r.iso_ctry_cd = 'US'
    ) ,--this query grabs the rtl_sales_thru and current month start dates for 2022 & 2023

    fleet_oos_mtd as (
    --jan
    SELECT 
    'Jan' as sales_mth_cat,
    1 as sales_mth_cat_nbr,
    f.rpt_dt as file_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.jan) as MTD_Sales
    from temp_foos f, rtl_sales_thru
    where f.sales_through_dt = rtl_sales_thru.sales_thru_dt
    --and rtl_sales_thru.rpt_mth = 1
    group by
    f.rpt_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt

    UNION
    --feb
    SELECT 
    'Feb' as sales_mth_cat,
    2 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.feb) as MTD_Sales
    from temp_foos f, rtl_sales_thru
    where f.sales_through_dt = rtl_sales_thru.sales_thru_dt
    --and rtl_sales_thru.rpt_mth = 2
    group by
    f.rpt_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt


    UNION
    --mar
    SELECT 
    'Mar' as sales_mth_cat,
    3 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.mar) as MTD_Sales
    from temp_foos f, rtl_sales_thru
    where f.sales_through_dt = rtl_sales_thru.sales_thru_dt
    --and rtl_sales_thru.rpt_mth = 3
    group by
    f.rpt_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt


    UNION
    --apr
    SELECT 
    'Apr' as sales_mth_cat,
    4 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.apr) as MTD_Sales
    from temp_foos f, rtl_sales_thru
    where f.sales_through_dt = rtl_sales_thru.sales_thru_dt
    --and rtl_sales_thru.rpt_mth = 4
    group by
    f.rpt_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt

    UNION
    --may
    SELECT 
    'May' as sales_mth_cat,
    5 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.may) as MTD_Sales
    from temp_foos f, rtl_sales_thru
    where f.sales_through_dt = rtl_sales_thru.sales_thru_dt
    --and rtl_sales_thru.rpt_mth = 5
    group by
    f.rpt_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt


    UNION
    --jun
    SELECT 
    'Jun' as sales_mth_cat,
    6 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.jun) as MTD_Sales
    from temp_foos f, rtl_sales_thru
    where f.sales_through_dt = rtl_sales_thru.sales_thru_dt
    --and rtl_sales_thru.rpt_mth = 6
    group by
    f.rpt_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt


    UNION
    --jul
    SELECT 
    'Jul' as sales_mth_cat,
    7 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.jul) as MTD_Sales
    from temp_foos f, rtl_sales_thru
    where f.sales_through_dt = rtl_sales_thru.sales_thru_dt
    --and rtl_sales_thru.rpt_mth = 7
    group by
    f.rpt_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt



    UNION
    --aug
    SELECT 
    'Aug' as sales_mth_cat,
    8 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.aug) as MTD_Sales
    from temp_foos f, rtl_sales_thru
    where f.sales_through_dt = rtl_sales_thru.sales_thru_dt
    --and rtl_sales_thru.rpt_mth = 8
    group by
    f.rpt_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt


    UNION
    --sep
    SELECT 
    'Sep' as sales_mth_cat,
    9 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.sep) as MTD_Sales
    from temp_foos f, rtl_sales_thru
    where f.sales_through_dt = rtl_sales_thru.sales_thru_dt
    --and rtl_sales_thru.rpt_mth = 9
    group by
    f.rpt_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt



    UNION
    --oct
    SELECT 
    'Oct' as sales_mth_cat,
    10 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.oct) as MTD_Sales
    from temp_foos f, rtl_sales_thru
    where f.sales_through_dt = rtl_sales_thru.sales_thru_dt
    --and rtl_sales_thru.rpt_mth = 10
    group by
    f.rpt_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt



    UNION
    --nov
    SELECT 
    'Nov' as sales_mth_cat,
    11 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.nov) as MTD_Sales
    from temp_foos f, rtl_sales_thru
    where f.sales_through_dt = rtl_sales_thru.sales_thru_dt
    --and rtl_sales_thru.rpt_mth = 11
    group by
    f.rpt_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt



    UNION
    --dec
    SELECT 
    'Dec' as sales_mth_cat,
    12 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.`dec`) as MTD_Sales
    from temp_foos f, rtl_sales_thru
    where f.sales_through_dt = rtl_sales_thru.sales_thru_dt
    --and rtl_sales_thru.rpt_mth = 12
    group by
    f.rpt_dt,
    rtl_sales_thru.sales_thru_dt,
    rtl_sales_thru.rpt_mth_nbr,
    rtl_sales_thru.rpt_mth_abbr_txt

    )

    select sales_mth_cat_nbr, sales_mth_cat, file_dt, sales_thru_dt, rpt_mth_nbr, rpt_mth_abbr_txt, MTD_Sales
    from fleet_oos_mtd
    where rpt_mth_nbr >= sales_mth_cat_nbr

    ;

          -----   MTD Historical    -----
     -------  Fleet Out Of Stock CDS  -------

"""


mtd_incremental = """

     -------  Fleet Out Of Stock CDS  -------
         -----   MTD Incremental   -----
        
    -- MTD Incremental Fleet OOS Sales

    -- Fleet OOS
    With inc_rtl_sales_thru as (
    select sales_thru_dt,
    curr_mth_start_dt,
    month(curr_mth_start_dt) as rpt_mth_nbr,
    date_format(curr_mth_start_dt, 'MMM') as rpt_mth_abbr_txt
    from temp_lookup 
    where sales_thru_dt = date_sub(to_date(current_date), 1)
    and iso_ctry_cd = 'US'), --This query provides the range of dates to be used

    fleet_oos_mtd as (
    --jan
    SELECT 
    'Jan' as sales_mth_cat,
    1 as sales_mth_cat_nbr,
    f.rpt_dt as file_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.jan) as MTD_Sales
    from temp_foos f, inc_rtl_sales_thru
    where f.sales_through_dt = inc_rtl_sales_thru.sales_thru_dt
    --and inc_rtl_sales_thru.rpt_mth = 1
    group by
    f.rpt_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt

    UNION
    --feb
    SELECT 
    'Feb' as sales_mth_cat,
    2 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.feb) as MTD_Sales
    from temp_foos f, inc_rtl_sales_thru
    where f.sales_through_dt = inc_rtl_sales_thru.sales_thru_dt
    --and inc_rtl_sales_thru.rpt_mth = 2
    group by
    f.rpt_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt


    UNION
    --mar
    SELECT 
    'Mar' as sales_mth_cat,
    3 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.mar) as MTD_Sales
    from temp_foos f, inc_rtl_sales_thru
    where f.sales_through_dt = inc_rtl_sales_thru.sales_thru_dt
    --and inc_rtl_sales_thru.rpt_mth = 3
    group by
    f.rpt_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt


    UNION
    --apr
    SELECT 
    'Apr' as sales_mth_cat,
    4 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.apr) as MTD_Sales
    from temp_foos f, inc_rtl_sales_thru
    where f.sales_through_dt = inc_rtl_sales_thru.sales_thru_dt
    --and inc_rtl_sales_thru.rpt_mth = 4
    group by
    f.rpt_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt

    UNION
    --may
    SELECT 
    'May' as sales_mth_cat,
    5 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.may) as MTD_Sales
    from temp_foos f, inc_rtl_sales_thru
    where f.sales_through_dt = inc_rtl_sales_thru.sales_thru_dt
    --and inc_rtl_sales_thru.rpt_mth = 5
    group by
    f.rpt_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt


    UNION
    --jun
    SELECT 
    'Jun' as sales_mth_cat,
    6 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.jun) as MTD_Sales
    from temp_foos f, inc_rtl_sales_thru
    where f.sales_through_dt = inc_rtl_sales_thru.sales_thru_dt
    --and inc_rtl_sales_thru.rpt_mth = 6
    group by
    f.rpt_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt


    UNION
    --jul
    SELECT 
    'Jul' as sales_mth_cat,
    7 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.jul) as MTD_Sales
    from temp_foos f, inc_rtl_sales_thru
    where f.sales_through_dt = inc_rtl_sales_thru.sales_thru_dt
    --and inc_rtl_sales_thru.rpt_mth = 7
    group by
    f.rpt_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt



    UNION
    --aug
    SELECT 
    'Aug' as sales_mth_cat,
    8 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.aug) as MTD_Sales
    from temp_foos f, inc_rtl_sales_thru
    where f.sales_through_dt = inc_rtl_sales_thru.sales_thru_dt
    --and inc_rtl_sales_thru.rpt_mth = 8
    group by
    f.rpt_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt


    UNION
    --sep
    SELECT 
    'Sep' as sales_mth_cat,
    9 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.sep) as MTD_Sales
    from temp_foos f, inc_rtl_sales_thru
    where f.sales_through_dt = inc_rtl_sales_thru.sales_thru_dt
    --and inc_rtl_sales_thru.rpt_mth = 9
    group by
    f.rpt_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt



    UNION
    --oct
    SELECT 
    'Oct' as sales_mth_cat,
    10 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.oct) as MTD_Sales
    from temp_foos f, inc_rtl_sales_thru
    where f.sales_through_dt = inc_rtl_sales_thru.sales_thru_dt
    --and inc_rtl_sales_thru.rpt_mth = 10
    group by
    f.rpt_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt



    UNION
    --nov
    SELECT 
    'Nov' as sales_mth_cat,
    11 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.nov) as MTD_Sales
    from temp_foos f, inc_rtl_sales_thru
    where f.sales_through_dt = inc_rtl_sales_thru.sales_thru_dt
    --and inc_rtl_sales_thru.rpt_mth = 11
    group by
    f.rpt_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt



    UNION
    --dec
    SELECT 
    'Dec' as sales_mth_cat,
    12 as sales_mth_cat_nbr,

    f.rpt_dt as file_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt,
    sum(f.`dec`) as MTD_Sales
    from temp_foos f, inc_rtl_sales_thru
    where f.sales_through_dt = inc_rtl_sales_thru.sales_thru_dt
    --and inc_rtl_sales_thru.rpt_mth = 12
    group by
    f.rpt_dt,
    inc_rtl_sales_thru.sales_thru_dt,
    inc_rtl_sales_thru.rpt_mth_nbr,
    inc_rtl_sales_thru.rpt_mth_abbr_txt

    )

    select sales_mth_cat_nbr, sales_mth_cat, file_dt, sales_thru_dt, rpt_mth_nbr, rpt_mth_abbr_txt, MTD_Sales
    from fleet_oos_mtd
    where rpt_mth_nbr >= sales_mth_cat_nbr

    ;
        
         -----   MTD Incremental   -----
     -------  Fleet Out Of Stock CDS  -------

"""


# Main Function
def main(ENV, script_name, detroit_tz, tidal_name, target_table, target_path):

    try:
        spark = initialize_spark_session(script_name)
    
        # Define Source Tables
        foos_table = "dl_edge_base_dcfpa_171749_base_dcfpa_avista.fleet_oos_stg"
        look_table = "marketing360_public.fs_gbl_sales_cal_day_rpt_lkp"
        create_temp_views(spark, look_table, foos_table)
        
        target_schema = [
            "Rpt_Dt",
            "Sales_Thru_Dt",
            "Sales_Mth_Cat_Nbr",
            "Src_Sys_Id",
            "Sales_Mth_Cat",
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

        # Daily_Sales Dataframe
        df_daily_historical  = spark.sql(daily_historical); df_daily_historical = df_daily_historical.withColumnRenamed("file_dt", "Rpt_Dt")
        df_daily_incremental = spark.sql(daily_incremental); df_daily_incremental = df_daily_incremental.withColumnRenamed("file_dt", "Rpt_Dt")
        df_daily             = df_daily_historical.union(df_daily_incremental)
        #info(df_daily)
        
        # MTD_Sales Dataframe
        df_mtd_historical    = spark.sql(mtd_historical); df_mtd_historical = df_mtd_historical.withColumnRenamed("file_dt", "Rpt_Dt")
        df_mtd_incremental   = spark.sql(mtd_incremental); df_mtd_incremental = df_mtd_incremental.withColumnRenamed("file_dt", "Rpt_Dt")
        df_mtd               = df_mtd_historical.union(df_mtd_incremental)
        #info(df_mtd)


        # Joining 'df_daily' & 'df_mtd'
        join_columns = [
            "Rpt_Dt",
            "Sales_Thru_Dt",
            "rpt_mth_nbr",
            "rpt_mth_abbr_txt",
            "Sales_Mth_Cat", 
            "Sales_Mth_Cat_Nbr"
        ]


        # Perform an inner join on the DataFrames
        joined_df = df_daily.join(df_mtd, join_columns)
        print("Printing 'joined_df' Schema")
        joined_df.printSchema()


        joined_df = joined_df.withColumn("Day_Sales_Cnt", col("Daily_Sales").cast("integer"))
        joined_df = joined_df.withColumn("Mth_To_Dt_Sales_Cnt", col("MTD_Sales").cast("integer"))


        # Define Plubming Columns
        SRC_SYS_ID     = F.lit("171749")
        MEAS_CNT       = F.lit(1)
        DW_ANOM_FLG    = F.lit("N")
        CURRENT_TS     = F.current_timestamp()
        DW_JOB_ID      = F.lit(f"{tidal_name}")
        SRC_SYS_IUD_CD = F.lit("I")
        SRC_SYS_UNIQ_PRIM_KEY_VAL     = F.concat(F.col("Rpt_Dt"), F.lit("|"), F.col("Sales_Thru_Dt"), F.lit("|"), F.col("Sales_Mth_Cat_Nbr"), F.lit("|"), SRC_SYS_ID)
        SRC_SYS_UNIQ_PRIM_KEY_COL_VAL = F.lit("Rpt_Dt|Sales_Thru_Dt|Sales_Mth_Cat_Nbr|Src_Sys_Id")


        print("Starting to create the final dataframe, 'df_final'")
        # Apply Plumbing Columns and Reorder as per target_schema
        df_final = (
            joined_df
            # PLUMBING COLUMNS
            .withColumn("Src_Sys_Id", SRC_SYS_ID)
            .withColumn("Src_Sys_Uniq_Prim_Key_Val", SRC_SYS_UNIQ_PRIM_KEY_VAL)
            .withColumn("Src_Sys_Uniq_Prim_Key_Col_Val", SRC_SYS_UNIQ_PRIM_KEY_COL_VAL)
            .withColumn("Meas_Cnt", MEAS_CNT)
            .withColumn("Dw_Anom_Flg", DW_ANOM_FLG)
            .withColumn("Dw_Mod_Ts", CURRENT_TS)
            .withColumn("Dw_Job_Id", DW_JOB_ID)
            .withColumn("Src_Sys_Iud_Cd", SRC_SYS_IUD_CD)
            .select(*target_schema)
        )
        #df_final.cache()
        df_final.printSchema()
        df_final.show(5)
        #COUNT_df_final = df_final.count()
        print("Final Dataframe 'df_final' has been created")

        print("Starting to write final dataframe")
        # Write to Target Table
        #print(f"Starting To Write, \033[38;2;0;153;204m'{COUNT_df_final}'\033[0m rows from 'df_final' to the Target-Table \033[38;2;0;153;204m'{target_table}'\033[0m")
        df_final.write.format('parquet').mode('overwrite').saveAsTable(target_table, path=target_path)
        #print(f"Finished Writing \033[38;2;0;153;204m'{COUNT_df_final}'\033[0m rows to the Target-Table")
        print("Finished Writing Final Dataframe")
        
    except Exception as e:
        print(f"Error executing ETL process: {e}")
        sys.exit(1) # Exit code '1' Indicating Failure
    finally:
        if spark is not None:
            spark.stop()
            
    sys.exit(0) # Exit Code '0' Indicating Success


if __name__ == "__main__":
    ENV          = sys.argv[1].lower()
    detroit_tz   = pytz.timezone('America/Detroit')
    script_name  = "CDS_FLEET_OUT_OF_STOCK_LRF"
    tidal_name   = "42124-edw-prd-sm-dcfpa-dsr-fleet-out-of-stock-lrf"
    target_table = f"sales_mktg_processing.{script_name}"
    target_path  = f"/sync/{ENV}_42124_edw_b/EDW/SALES_MKTG/RZ/{script_name}/Processing"

    main(ENV, detroit_tz, script_name, tidal_name, target_table, target_path)