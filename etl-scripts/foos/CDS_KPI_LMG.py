#!/usr/bin/env python
# coding: utf-8
"""
Fleet Out-of-Stock KPIs Calculation Script
===========================================

Purpose:
--------
This Python script calculates various Key Performance Indicators (KPIs) for fleet out-of-stock data using PySpark.
The calculations, traditionally performed using SQL, have been implemented in PySpark for enhanced scalability
and performance with large datasets.

Key Features:
-------------
1. **Dynamic Spark Session Initialization**:
   - Configures Spark dynamically with optimal parameters for memory, cores, and parallelism.
   - Enables Hive support for querying data stored in Hive tables.

2. **Date Variable Initialization**:
   - Establishes Detroit timezone as a reference for all date calculations.
   - Generates current date, time, and related variables for filtering and timestamping data.

3. **Data Loading and Transformation**:
   - Loads lookup and fleet out-of-stock staging tables from Hive.
   - Constructs temporary views and performs SQL-style transformations on PySpark DataFrames.

4. **KPI Calculations**:
   - **MTD (Month-to-Date) Sales**: Calculates cumulative sales within the current month.
   - **YTD (Year-to-Date) Sales**: Summarizes sales from the beginning of the year up to the current date.
   - **Prior Year Comparisons**: Compares current YTD sales with those of the same period in the previous year.
   - **Variance and Percentage Change**: Computes differences and percentage changes between current and prior YTD sales.

5. **Filling Missing Dates**:
   - Handles date gaps in the fleet out-of-stock data by filling missing values with appropriate defaults using window functions.

6. **Dynamic Aggregation**:
   - Aggregates monthly and daily sales data with support for varying date ranges.
   - Joins fleet data with lookup tables for enriched insights.

7. **Performance Optimizations**:
   - Uses Spark SQL and DataFrame APIs with caching for efficient computation.
   - Configures Kryo serialization and optimizes shuffle partitions.

Inputs:
-------
1. **Hive Tables**:
   - `marketing360_public.fs_vsna_dim_selling_day_report_lkup`: Lookup data with date-related mappings.
   - `pyspark_pitstop.fleet_oos_stg`: Fleet out-of-stock staging data.

2. **Date and Environment Variables**:
   - Current date string is dynamically initialized based on the Detroit timezone.
   - Accepts environment variables (`env`) for determining runtime configurations.

Outputs:
--------
- The script processes and displays transformed DataFrames with calculated KPIs for verification and further usage.

Author:
-------
- Levi Gagne
"""

import sys, datetime, pytz, calendar
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

def initialize_spark_session(app_name="SZ92Q9"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.caseSensitive", "true")  # Case sensitivity in SQL queries
        .config("spark.sql.parquet.binaryAsString", "true")  # Handling binary data in Parquet files
        .config("spark.dynamicAllocation.enabled", "true")  # Dynamic allocation of executors
        .config("spark.dynamicAllocation.initialExecutors", 4)  # Initial number of executors
        .config("spark.dynamicAllocation.minExecutors", 2)  # Minimum number of executors
        .config("spark.dynamicAllocation.maxExecutors", 20)  # Maximum number of executors
        .config("spark.executor.memory", "8g")  # Memory per executor
        .config("spark.executor.cores", 4)  # Number of cores per executor
        .config("spark.network.timeout", "800s")  # Network timeout setting
        .config("spark.sql.shuffle.partitions", "200")  # Number of shuffle partitions
        .config("spark.default.parallelism", "200")  # Default parallelism level
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  # Serializer class
        .config("spark.kryoserializer.buffer.max", "1024m")  # Maximum buffer size for Kryo
        .config("spark.sql.execution.arrow.enabled", "true")  # Enable Arrow for Pandas conversion
        .enableHiveSupport()  # Enable Hive support for Spark SQL
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('ERROR')  # Set log level to reduce verbosity
    print(f"\033[38;2;0;153;204mSpark Version:\033[0m {spark.version},", f"\033[38;2;0;255;128mPython Version:\033[0m {sys.version.split()[0]}") # Print Spark and Python versions for reference
    return spark


def initialize_date_variables():
    
    detroit_tz           = pytz.timezone('America/Detroit')  # Detroit Time Zone
    current_date_detroit = datetime.datetime.now(detroit_tz) # Current date in Detroit

    # Preparing date-related variables
    date_vars = {
        'DETRIOT': detroit_tz,
        'CURRENT_DATE_DETROIT': current_date_detroit,
        'CURRENT_YEAR': current_date_detroit.year,
        'CURRENT_MONTH': f"{current_date_detroit.month:02d}",
        'CURRENT_DAY': f"{current_date_detroit.day:02d}",
        'CURRENT_TIMESTAMP_STR': current_date_detroit.strftime("%Y-%m-%d %H:%M:%S"),
        'CURRENT_DATE_STRING': current_date_detroit.strftime("%Y-%m-%d")  # String representation in 'yyyy-mm-dd'
    }
    # Printing the date variables
    for key, value in date_vars.items():
        print(f"{key}: {value}")
    return date_vars


def create_lookup_data(spark, look_table):
    df_lookup = spark.read.table(look_table)
    df_lookup.createOrReplaceTempView("temp_lookup")

    # Predefined list of columns with improved readability
    columns_to_select = [
        "salesthroughdate AS sales_through_date", 
        "currentmonthmthstart AS current_month_mth_start_date",
        "priormthcurrentday AS prior_mth_current_day_date",
        "dayssupplysellingday AS days_supply_selling_day",
        "priormthstart As prior_mth_start_date",
        "prioryearmthend As prior_year_mth_end_date",
        "prioryearmthstart As prior_year_mth_start_date",
        "currentmonthnumbersellingdays As current_month_number_selling_days",
        "currentmonthcumselldays As current_month_cum_sell_days",
        "currentyearmthlookupstart As current_year_mth_lookup_start_date",
        "prioryearcurrentday As prior_year_current_day_date"
    ]

    select_clause = ", ".join(columns_to_select) # Construct the SQL select clause from the columns list

    lookup_data = spark.sql(f"""
        SELECT 
            {select_clause}
        FROM temp_lookup
    """).cache()
    
    return lookup_data


def create_fleet_oos(spark, foos_table):
    
    month_list = [calendar.month_abbr[i] for i in range(1, 13)] # Define the list of months within the function

    df_foos = spark.read.table(foos_table)
    df_foos.createOrReplaceTempView("temp_foos")

    # Fetch multiple fields from 'fleet_oos_stg' and alias as 'fleet_OOS'
    fleet_oos = spark.sql("""
        SELECT 
            Rpt_Dt, 
            Sales_Through_Dt, 
            Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, `Dec`
        FROM temp_foos AS fleet_OOS
    """)

    # Aggregate Monthly Sales by Report Date
    agg_exprs = [F.sum(month).alias(month) for month in month_list]
    agg_fleet_oos = fleet_oos.groupBy("Rpt_Dt", "Sales_Through_Dt").agg(*agg_exprs)

    return agg_fleet_oos



def create_monday_filled_agg_fleet_oos_with_lookup(spark, agg_fleet_oos, lookup_data, current_date_string):

    end_date   = datetime.datetime.strptime(current_date_string, "%Y-%m-%d").date() # Convert the current_date_string to a datetime.date object
    month_list = [calendar.month_abbr[i] for i in range(1, 13)] # Define MONTH_LIST within the function
    
    # Extract the start date and convert it to a datetime.date object
    start_date = agg_fleet_oos.agg(F.min("Rpt_Dt")).first()[0]
    start_date = start_date.date() if isinstance(start_date, datetime.datetime) else start_date

    # Generate the complete date range
    date_range         = [start_date + datetime.timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    date_df            = spark.createDataFrame(date_range, DateType()).toDF("Full_Rpt_Dt")
    full_agg_fleet_oos = date_df.join(agg_fleet_oos, date_df.Full_Rpt_Dt == agg_fleet_oos.Rpt_Dt, "left_outer") # Left join with the agg_fleet_oos DataFrame

    windowSpec = Window.orderBy("Full_Rpt_Dt") # Create a Window Specification ordered by 'Full_Rpt_Dt'

    # Fill in missing values by carrying forward the last known values
    filled_agg_fleet_oos = full_agg_fleet_oos \
        .withColumn("Rpt_Dt", F.coalesce(full_agg_fleet_oos.Rpt_Dt, full_agg_fleet_oos.Full_Rpt_Dt)) \
        .withColumn("Sales_Through_Dt", F.last("Sales_Through_Dt", ignorenulls=True).over(windowSpec))

    month_list = [calendar.month_abbr[i] for i in range(1, 13)]  # Define the month list
    for month in month_list:
        filled_agg_fleet_oos = filled_agg_fleet_oos.withColumn(month, F.last(month, ignorenulls=True).over(windowSpec)) # Fill in missing values for monthly columns
        
    # Join filled_agg_fleet_oos with lookup_data using the correct column name
    filled_fleet_oos_with_lookup = filled_agg_fleet_oos.join(
        lookup_data,
        filled_agg_fleet_oos.Sales_Through_Dt == lookup_data.sales_through_date,
        'inner'
    )
    
    monday_filled_fleet_oos_with_lookup = filled_fleet_oos_with_lookup.select(
        filled_agg_fleet_oos["Rpt_Dt"], 
        *[filled_agg_fleet_oos[month] for month in month_list],
        filled_agg_fleet_oos["Sales_Through_Dt"],
        lookup_data["sales_through_date"],
        lookup_data["current_month_mth_start_date"],
        lookup_data["prior_mth_current_day_date"],
        lookup_data["days_supply_selling_day"],
        lookup_data["prior_mth_start_date"],
        lookup_data["prior_year_mth_end_date"],
        lookup_data["prior_year_mth_start_date"],
        lookup_data["prior_year_current_day_date"],
        lookup_data["current_month_number_selling_days"],
        lookup_data["current_month_cum_sell_days"]
    )
    return monday_filled_fleet_oos_with_lookup



def create_agg_fleet_oos_with_lookup(spark, agg_fleet_oos, lookup_data, current_date_string):
    
    month_list = [calendar.month_abbr[i] for i in range(1, 13)]
    
    # Join agg_fleet_oos with lookup_data
    agg_fleet_oos_with_lookup = agg_fleet_oos.join(
        lookup_data,
        agg_fleet_oos.Sales_Through_Dt == lookup_data.sales_through_date,
        'inner'
    )

    # Select necessary columns from both DataFrames
    agg_fleet_oos_with_lookup = agg_fleet_oos_with_lookup.select(
        agg_fleet_oos["Rpt_Dt"], 
        *[agg_fleet_oos[month] for month in month_list],
        agg_fleet_oos["Sales_Through_Dt"],
        lookup_data["sales_through_date"],
        lookup_data["current_month_mth_start_date"],
        lookup_data["prior_mth_current_day_date"],
        lookup_data["days_supply_selling_day"],
        lookup_data["prior_mth_start_date"],
        lookup_data["prior_year_mth_end_date"],
        lookup_data["prior_year_mth_start_date"],
        lookup_data["prior_year_current_day_date"],
        lookup_data["current_month_number_selling_days"],
        lookup_data["current_month_cum_sell_days"]
    )
    return agg_fleet_oos_with_lookup



def calculate_mtd_sales(fleet_oos_with_lookup, current_date_string):
    # Calculate MTD Sales
    agg_fleet_oos_with_mtd = fleet_oos_with_lookup.withColumn(
        "mtd_sales",
        F.expr("""
            CASE
                WHEN MONTH(current_month_mth_start_date) = 1 AND Rpt_Dt = current_month_mth_start_date THEN Dec
                WHEN MONTH(current_month_mth_start_date) = 2 AND Rpt_Dt = current_month_mth_start_date THEN Jan
                WHEN MONTH(current_month_mth_start_date) = 3 AND Rpt_Dt = current_month_mth_start_date THEN Feb
                WHEN MONTH(current_month_mth_start_date) = 4 AND Rpt_Dt = current_month_mth_start_date THEN Mar
                WHEN MONTH(current_month_mth_start_date) = 5 AND Rpt_Dt = current_month_mth_start_date THEN Apr
                WHEN MONTH(current_month_mth_start_date) = 6 AND Rpt_Dt = current_month_mth_start_date THEN May
                WHEN MONTH(current_month_mth_start_date) = 7 AND Rpt_Dt = current_month_mth_start_date THEN Jun
                WHEN MONTH(current_month_mth_start_date) = 8 AND Rpt_Dt = current_month_mth_start_date THEN Jul
                WHEN MONTH(current_month_mth_start_date) = 9 AND Rpt_Dt = current_month_mth_start_date THEN Aug
                WHEN MONTH(current_month_mth_start_date) = 10 AND Rpt_Dt = current_month_mth_start_date THEN Sep
                WHEN MONTH(current_month_mth_start_date) = 11 AND Rpt_Dt = current_month_mth_start_date THEN Oct
                WHEN MONTH(current_month_mth_start_date) = 12 AND Rpt_Dt = current_month_mth_start_date THEN Nov
                ELSE
                    CASE
                        WHEN MONTH(current_month_mth_start_date) = 1 THEN Jan
                        WHEN MONTH(current_month_mth_start_date) = 2 THEN Feb
                        WHEN MONTH(current_month_mth_start_date) = 3 THEN Mar
                        WHEN MONTH(current_month_mth_start_date) = 4 THEN Apr
                        WHEN MONTH(current_month_mth_start_date) = 5 THEN May
                        WHEN MONTH(current_month_mth_start_date) = 6 THEN Jun
                        WHEN MONTH(current_month_mth_start_date) = 7 THEN Jul
                        WHEN MONTH(current_month_mth_start_date) = 8 THEN Aug
                        WHEN MONTH(current_month_mth_start_date) = 9 THEN Sep
                        WHEN MONTH(current_month_mth_start_date) = 10 THEN Oct
                        WHEN MONTH(current_month_mth_start_date) = 11 THEN Nov
                        WHEN MONTH(current_month_mth_start_date) = 12 THEN Dec
                    END
            END
        """)
    )

    # Calculating Daily Sales From MTD Sales
    final_df = agg_fleet_oos_with_mtd.select("Rpt_Dt", "current_month_mth_start_date", "mtd_sales")
    sorted_final_df = final_df.orderBy(F.desc("Rpt_Dt"))

    # Windowing & Lag Operations
    windowSpec = Window.orderBy("Rpt_Dt")
    sorted_final_df = (sorted_final_df
                       .withColumn("prev_day", F.date_add("Rpt_Dt", -1))
                       .withColumn("prev_day_mtd_sales", F.lag("mtd_sales").over(windowSpec)))

    # Prepare the final DataFrame
    df_SQL = sorted_final_df.select(
        F.col("Rpt_Dt").alias("Rpt_Dt"),
        F.col("prev_day").alias("Sales_Thru_Dt"),
        F.col("current_month_mth_start_date"), 
        F.when(
            (F.col("prev_day") == F.col("current_month_mth_start_date")) & (F.month("current_month_mth_start_date") == 1), 0
        ).when(
            F.col("prev_day") == F.col("current_month_mth_start_date"), F.col("mtd_sales")
        ).when(
            F.col("prev_day").isNull(), F.col("mtd_sales")
        ).otherwise(
            F.col("mtd_sales") - F.col("prev_day_mtd_sales")
        ).alias("Day_Sales_Cnt"),
        F.col("mtd_sales").alias("Mth_To_Dt_Sales_Cnt")
    ).orderBy(
        F.desc("Rpt_Dt")
    )

    # Drop duplicates and sum up Day_Sales_Cnt
    df_MERGED = df_SQL.groupBy("Rpt_Dt").agg(
        F.first("Day_Sales_Cnt", ignorenulls=True).alias("Day_Sales_Cnt"),
        F.first("Mth_To_Dt_Sales_Cnt", ignorenulls=True).alias("Mth_To_Dt_Sales_Cnt"),
        F.first("current_month_mth_start_date", ignorenulls=True).alias("Curr_Mth_Start_Dt")
    )
    
    # Filter df_MERGED to include only rows where Rpt_Dt equals current_date_string
    df_Daily_Sales = df_MERGED.filter(df_MERGED.Rpt_Dt == current_date_string).select("Rpt_Dt", "Day_Sales_Cnt")

    return df_Daily_Sales


def calculate_mtd_sales_for_current_day(df, current_date_string):

    # Mapping of month numbers to month names
    month_names_dictionary = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }

    # Filter the DataFrame for the current date
    filtered_df = df.filter(df.Rpt_Dt == current_date_string)
    filtered_row = filtered_df.collect()[0]

    # Extract the necessary date components
    curr_mth_start_dt = filtered_row['current_month_mth_start_date']
    curr_mth_start_month = curr_mth_start_dt.month

    # Get the string representation of the month
    month_str = month_names_dictionary[curr_mth_start_month]

    # Determine the MTD sales
    if filtered_row['sales_through_date'] == curr_mth_start_dt:
        if month_str == "Jan":
            mtd_sales = 0
        else:
            mtd_sales = filtered_row[month_str]
    else:
        mtd_sales = filtered_row[month_str]

    # Create a new DataFrame with the calculated values
    return spark.createDataFrame([(filtered_row['Rpt_Dt'], mtd_sales)], ["Rpt_Dt", "MTD_Sales"])


def calculate_ytd_sales(data_frame, current_date_str, spark_session):
    month_list = [calendar.month_abbr[i] for i in range(1, 13)]
    # Filter the DataFrame for the current date
    filtered_df = data_frame.filter(data_frame['Rpt_Dt'] == current_date_str)
    filtered_row = filtered_df.collect()[0]

    # Extract the month from the current date
    current_month = datetime.datetime.strptime(current_date_str, "%Y-%m-%d").month

    # Sum the sales from January up to the current month
    ytd_sales = 0
    for month in month_list[:current_month]:
        month_sales = filtered_row[month]
        if month_sales is not None:
            ytd_sales += month_sales

    # Create a DataFrame with the result
    ytd_sales_df = spark_session.createDataFrame([Row(Rpt_Dt=current_date_str, YTD_Sales=ytd_sales)])
    return ytd_sales_df


def calculate_prior_ytd_sales(data_frame, current_date_str, spark_session):
    month_list = [calendar.month_abbr[i] for i in range(1, 13)]
    
    # Filter the DataFrame for the current date
    filtered_df = data_frame.filter(data_frame['Rpt_Dt'] == current_date_str)
    filtered_row = filtered_df.collect()[0]
    
    # Extract the value in the prior_year_current_day_date column
    prior_year_current_day_date = filtered_row['prior_year_current_day_date']
    # Increment the date by one day
    prior_year_current_day_date += timedelta(days=1)
    # Ensure prior_year_date is in the correct string format
    prior_year_date_str = prior_year_current_day_date.strftime("%Y-%m-%d")
    
    filtered_df = data_frame.filter(data_frame['Rpt_Dt'] == prior_year_date_str)
    filtered_row = filtered_df.collect()[0]

    # Calculate prior YTD sales using the prior year's date
    current_month = datetime.datetime.strptime(prior_year_date_str, "%Y-%m-%d").month
    prior_ytd_sales = 0
    for month in month_list[:current_month]:
        month_sales = filtered_row[month]
        if month_sales is not None:
            prior_ytd_sales += month_sales

    # Create a DataFrame with the result
    prior_ytd_sales_df = spark_session.createDataFrame([Row(Rpt_Dt=prior_year_date_str, PRIOR_YTD_Sales=prior_ytd_sales)])
    return prior_ytd_sales_df


def calculate_variance(data_frame, current_date_str, spark_session):
    # Calculate current YTD sales
    ytd_sales_df = calculate_ytd_sales(data_frame, current_date_str, spark_session)
    ytd_sales_row = ytd_sales_df.collect()[0]
    ytd_sales = ytd_sales_row['YTD_Sales']

    # Calculate prior YTD sales
    prior_ytd_sales_df = calculate_prior_ytd_sales(data_frame, current_date_str, spark_session)
    prior_ytd_sales_row = prior_ytd_sales_df.collect()[0]
    prior_ytd_sales = prior_ytd_sales_row['PRIOR_YTD_Sales']

    # Calculate Variance
    variance = ytd_sales - prior_ytd_sales

    # Create a DataFrame with the result
    variance_df = spark_session.createDataFrame([Row(Rpt_Dt=current_date_str, Variance=variance)])

    return variance_df


def calculate_percentage_change(data_frame, current_date_str, spark_session):
    # Calculate current YTD sales
    ytd_sales_df = calculate_ytd_sales(data_frame, current_date_str, spark_session)
    ytd_sales_row = ytd_sales_df.collect()[0]
    ytd_sales = ytd_sales_row['YTD_Sales']

    # Calculate prior YTD sales
    prior_ytd_sales_df = calculate_prior_ytd_sales(data_frame, current_date_str, spark_session)
    prior_ytd_sales_row = prior_ytd_sales_df.collect()[0]
    prior_ytd_sales = prior_ytd_sales_row['PRIOR_YTD_Sales']
    current_YTD_Sales_DIFF_prior_YTD_Sales = ytd_sales - prior_ytd_sales
    # Calculate Percent Change
    # Ensure the denominator (prior YTD sales) is not zero to avoid division by zero error
    if prior_ytd_sales != 0:
        percent_change = (current_YTD_Sales_DIFF_prior_YTD_Sales / prior_ytd_sales)
    else:
        percent_change = None  # Assign None or a suitable value if prior YTD sales is zero

    # Create a DataFrame with the result
    percent_change_df = spark_session.createDataFrame([Row(Rpt_Dt=current_date_str, Percent_Change=percent_change)])

    return percent_change_df




def main(script_name, env, tidal_name):
    try:
        # Initialize the Spark session with the script name
        spark = initialize_spark_session(script_name)
        
        date_vars  = initialize_date_variables() # Initialize and print date variables
        month_list = [calendar.month_abbr[i] for i in range(1, 13)]  # Ensure this matches the MONTH_LIST used in your working code
        look_table = "marketing360_public.fs_vsna_dim_selling_day_report_lkup"
        foos_table = "pyspark_pitstop.fleet_oos_stg"

        lookup_data   = create_lookup_data(spark, look_table)
        agg_fleet_oos = create_fleet_oos(spark, foos_table)
        
        # Call the combined function with CURRENT_DATE_STRING
        monday_filled_fleet_oos_with_lookup = create_monday_filled_agg_fleet_oos_with_lookup(spark, agg_fleet_oos, lookup_data, date_vars['CURRENT_DATE_STRING'])
        #monday_filled_fleet_oos_with_lookup.printSchema()
        #monday_filled_fleet_oos_with_lookup.show(5)
        
        # Call the function to create AGG_fleet_oos_with_lookup DataFrame
        agg_fleet_oos_with_lookup = create_agg_fleet_oos_with_lookup(spark, agg_fleet_oos, lookup_data, date_vars['CURRENT_DATE_STRING'])
        agg_fleet_oos_with_lookup.printSchema()
        agg_fleet_oos_with_lookup.show(5)
        
    except Exception as e:
        sys.exit(f"ETL process failed due to error: {e}")

# Entry point of the script
if __name__ == "__main__":
    script_name  = "fleet_out_of_Stock"  # This can be replaced with any desired script name
    env          = sys.argv[1].lower()    # Environment variable from command line
    tidal_name   = "42124-edw-prd-sm-dcfpa-dsr-ret-crtsy-trp-pgm-lrf"  # Some identifier

    main(script_name, env, tidal_name)
