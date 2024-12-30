#!/usr/bin/env python
# coding: utf-8


# PYTHON STANDARD LIBRARY
import sys, time, pytz, calendar, inspect
from datetime import datetime, timedelta
# THIRD-PARTY IMPORTS
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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


def create_temp_views(spark, RetC_table, M360_table, V360_table):
    """
    Creates temporary views from the given source-table names.
    """
    df_RetC = spark.read.table(RetC_table)
    df_M360 = spark.read.table(M360_table)
    df_V360 = spark.read.table(V360_table)

    df_RetC.createOrReplaceTempView("temp_RetC")
    df_M360.createOrReplaceTempView("temp_M360")
    df_V360.createOrReplaceTempView("temp_V360")



def create_plumbing_columns(tidal_name, detroit_tz):
    """
    Generates plumbing columns and returns them in a dictionary.

    :param tidal_name: Name of the Tidal job.
    :param detroit_tz: Timezone for Detroit.
    """
    current_ts_detroit = F.from_utc_timestamp(F.current_timestamp(), detroit_tz.zone).alias('CURRENT_TS')

    return {
        'SRC_SYS_ID': F.lit("171749"),
        'MEAS_CNT': F.lit(1),
        'DW_ANOM_FLG': F.lit("N"),
        'CURRENT_TS': current_ts_detroit,
        'DW_JOB_ID': F.lit(f"{tidal_name}"),
        'SRC_SYS_IUD_CD': F.lit("I"),
        'SRC_SYS_UNIQ_PRIM_KEY_VAL': F.concat(F.col("Veh_Id_Nbr"), F.lit("|"), F.col("File_Dt"), F.lit("|"), F.col("Out_Svc_Dt"), F.lit("|"), F.lit("171749")),
        'SRC_SYS_UNIQ_PRIM_KEY_COL': F.lit("Veh_Id_Nbr|File_Dt|Out_Svc_Dt|Src_Sys_Id")
    }



CTP_HISTORICAL_SQL = """

    ------  COURTESY TRANSPORTATION CDS  ------

          -----   HISTORICAL SQL   -----

    With rtl_sales_thru as (
        select sales_thru_dt, --Sales thru Date
            prior_yr_start_dt --Prior Years
        from temp_M360
        where iso_ctry_cd = 'US' 
        and sales_thru_dt = date_sub(to_date(current_date),1)
    ), --this query grabs the rtl_sales_thru date ranges and selling day count


      Stg_Out_Serv as (
      SELECT vin, outdate, todaydate, daysinservice, outmile, outtype, loangreaterthan60,

      row_number() over 
                (partition by vin || outdate || daysinservice || outmile
                order by todaydate desc) row_num -- identify duplicate entries and choose most recent file date as row 1

      FROM temp_RetC

      ), -- pull all data from staging table and identify duplicates



        In_Serv_Dtl as (
        Select
        DISTINCT f.veh_id_nbr,
        f.orig_proc_dt,
         f.biz_assoc_id,
            f.model_yr,
            case f.sell_src_cd
                when 11 then 'Buick'
                when 12 then 'Cadillac'
                when 13 then 'Chevrolet'
                when 48 then 'GMC'
            end as selling_source,
            f.sell_src_cd,
            f.veh_prod_rpt_model_cd,
            f.deliv_type_ctgy,
            row_number() over 
                (partition by f.veh_id_nbr || f.biz_assoc_id
                order by f.orig_proc_dt asc) row_num -- identify all process dates for CTP event/vin

        from
            temp_V360 f, rtl_sales_thru


        where
            f.ntl_cd = 'US' --reporting country
           and f.kpi_type_cd in (25,95)
            and f.sell_src_cd in (11,12,13,48) --Buick, Cadillac, Chevrolet, GMC
           and year(f.orig_proc_dt) >= 2021 
          --and datediff(to_date(rtl_sales_thru.prior_yr_start_dt), f.orig_proc_dt) < 365  -- no records earlier than Jan 2021

          and f.veh_deliv_cnt = 1

        ), -- pull all CTP records (+1) back to January 2021

    Dlr_Seq as (
        Select
        veh_id_nbr,
        orig_proc_dt,
        biz_assoc_id,
        model_yr,
        selling_source,
        sell_src_cd,
        veh_prod_rpt_model_cd,
        row_number() over 
                (partition by veh_id_nbr
                order by orig_proc_dt asc) in_dlr_num,  -- first dealer with CTP event for vin
        row_number() over 
                (partition by veh_id_nbr
                order by orig_proc_dt desc) out_dlr_num -- most recent dealer with CTP event for vin


        from In_Serv_Dtl

        where row_num = 1 -- First process date for Dlr/CTP event

        ),  --Orders Dlr/VIN events

    In_Serv_Dlr_Dtl as (

    Select

        'In_Dlr' as category,
        Stg_Out_Serv.vin,
        incpt.orig_proc_dt,
        Stg_Out_Serv.outdate,
        Stg_Out_Serv.daysinservice,
        Stg_Out_Serv.todaydate,
        incpt.biz_assoc_id,
        incpt.selling_source, 
        incpt.sell_src_cd,
        incpt.veh_prod_rpt_model_cd,
        incpt.model_yr,
        Stg_Out_Serv.outmile,
        Stg_Out_Serv.outtype,
        Stg_Out_Serv.loangreaterthan60,
        row_number() over 
                (partition by vin
                order by todaydate asc) dlr_seq_num -- -- number the sequence of events per vin by file date

    from
       Stg_Out_Serv

    left join
        Dlr_Seq incpt
        on incpt.veh_id_nbr = Stg_Out_Serv.vin


    where Stg_Out_Serv.todaydate <= date_sub(to_date(current_date),1)
    and Stg_Out_Serv.row_num = 1                    -- eliminate duplicate ontrac record and choose most recent record
    and incpt.in_dlr_num = 1                        -- select first dealer/bac with CTP event for vin
    and Stg_Out_Serv.outdate > incpt.orig_proc_dt   -- outdate must occur after process date



    group by

        'In_Dlr',
        Stg_Out_Serv.vin,
        incpt.orig_proc_dt,
        Stg_Out_Serv.outdate,
        Stg_Out_Serv.daysinservice,
        Stg_Out_Serv.todaydate,
        incpt.biz_assoc_id,
        incpt.selling_source, 
        incpt.sell_src_cd,
        incpt.veh_prod_rpt_model_cd,
        incpt.model_yr,
        Stg_Out_Serv.outmile,
        Stg_Out_Serv.outtype,
        Stg_Out_Serv.loangreaterthan60


    ),--Record for first dealer/first OnTrac record


    Out_Serv_Dlr_Dtl as (
    Select
        'Out_Dlr' as category,
        Stg_Out_Serv.vin,
        outcpt.orig_proc_dt,
        Stg_Out_Serv.outdate,
        Stg_Out_Serv.daysinservice,
        Stg_Out_Serv.todaydate,
        outcpt.biz_assoc_id,
        outcpt.selling_source, 
        outcpt.sell_src_cd,
        outcpt.veh_prod_rpt_model_cd,
        outcpt.model_yr,
        Stg_Out_Serv.outmile,
        Stg_Out_Serv.outtype,
        Stg_Out_Serv.loangreaterthan60,
        row_number() over 
                (partition by vin
                order by todaydate desc) dlr_seq_num -- number the sequence of events per vin by file date

    from
       Stg_Out_Serv

    left join
        Dlr_Seq outcpt
        on outcpt.veh_id_nbr = Stg_Out_Serv.vin

    where Stg_Out_Serv.todaydate <= date_sub(to_date(current_date),1)
    and Stg_Out_Serv.row_num = 1                    -- eliminate duplicate ontrac record and choose most recent record
    and outcpt.out_dlr_num = 1                        -- select last dealer/bac with CTP event for vin
    and Stg_Out_Serv.outdate > outcpt.orig_proc_dt   -- outdate must occur after process date



    group by
        'Out_Dlr',
        Stg_Out_Serv.vin,
        outcpt.orig_proc_dt,
        Stg_Out_Serv.outdate,
        Stg_Out_Serv.daysinservice,
        Stg_Out_Serv.todaydate,
        outcpt.biz_assoc_id,
        outcpt.selling_source, 
        outcpt.sell_src_cd,
        outcpt.veh_prod_rpt_model_cd,
        outcpt.model_yr,
        Stg_Out_Serv.outmile,
        Stg_Out_Serv.outtype,
        Stg_Out_Serv.loangreaterthan60
    ), -- chooses most recent Dlr/CT record

    Comb_CT_Dlr as (

    select
        category,
        vin,
        orig_proc_dt,
        outdate,
        daysinservice,
        todaydate,
        biz_assoc_id,
        selling_source, 
        sell_src_cd,
        veh_prod_rpt_model_cd,
        model_yr,
        outmile,
        outtype,
        loangreaterthan60,
        dlr_seq_num


    From In_Serv_Dlr_Dtl 


    where dlr_seq_num = 1   -- choose first record for sequence of retired out event


    group by
        category,
        vin,
        orig_proc_dt,
        outdate,
        daysinservice,
        todaydate,
        biz_assoc_id,
        selling_source, 
        sell_src_cd,
        veh_prod_rpt_model_cd,
        model_yr,
        outmile,
        outtype,
        loangreaterthan60,
        dlr_seq_num


    UNION

    select
        category,
        vin,
        orig_proc_dt,
        outdate,
        daysinservice,
        todaydate,
        biz_assoc_id,
        selling_source, 
        sell_src_cd,
        veh_prod_rpt_model_cd,
        model_yr,
        outmile,
        outtype,
        loangreaterthan60,
            dlr_seq_num


    From Out_Serv_Dlr_Dtl  


    where 
    vin || biz_assoc_id || todaydate not in (select vin || biz_assoc_id || todaydate FROM In_Serv_Dlr_Dtl where dlr_seq_num = 1) 
    and vin || outdate || todaydate not in (select vin || outdate || todaydate FROM In_Serv_Dlr_Dtl where dlr_seq_num = 1)
    -- chooses all other retired out event records excluding the first record to identify any changes in bac or outdate


    group by
        category,
        vin,
        orig_proc_dt,
        outdate,
        daysinservice,
        todaydate,
        biz_assoc_id,
        selling_source, 
        sell_src_cd,
        veh_prod_rpt_model_cd,
        model_yr,
        outmile,
        outtype,
        loangreaterthan60,
            dlr_seq_num

      ) -- give full list of retired out records with corresponding bac and vehicle model info

     select
        vin as veh_id_nbr,
        todaydate as file_dt,
        orig_proc_dt as in_svc_dt,
        outdate as out_svc_dt,
        biz_assoc_id as biz_assoc_id,
        daysinservice as day_in_svc_cnt,
        outmile as out_svc_mil_cnt,
        outtype as out_svc_type_cd,
        selling_source as sell_src_desc, 
        sell_src_cd as sell_src_cd,
        veh_prod_rpt_model_cd as veh_prod_rpt_model_cd,
        model_yr as model_yr,
        loangreaterthan60 as loan_great_than_60_day_cd


    From Comb_CT_Dlr    

    group by
        vin,
        todaydate,
        orig_proc_dt,
        outdate,
        biz_assoc_id,
        daysinservice,
        outmile,
        outtype,
        selling_source, 
        sell_src_cd,
        veh_prod_rpt_model_cd,
        model_yr,
        loangreaterthan60

    ---- final column names -----


          -----   HISTORICAL SQL   -----

    ------  COURTESY TRANSPORTATION CDS  ------

"""


CTP_INCREMENTAL_SQL = """

     ------  COURTESY TRANSPORTATION CDS  ------

       -----       INCREMENTAL SQL     -----

    With rtl_sales_thru as (
        select sales_thru_dt, --Sales thru Date
            prior_yr_start_dt --Prior Years
        from temp_M360
        where iso_ctry_cd = 'US' 
        and sales_thru_dt = date_sub(to_date(current_date),1)
    ), --this query grabs the rtl_sales_thru date ranges and selling day count



      Inc_Stg_Out_Serv as (
      SELECT vin, outdate, todaydate, daysinservice, outmile, outtype, loangreaterthan60,

      row_number() over 
                (partition by vin || outdate || daysinservice || outmile
                order by todaydate asc) row_num -- identify duplicate entries and choose most recent file date as row 1

      FROM temp_RetC

      ),    -- pull all data from staging table and identify duplicates



        In_Serv_Dtl as (
        Select
        DISTINCT f.veh_id_nbr,
        f.orig_proc_dt,
         f.biz_assoc_id,
            f.model_yr,
            case f.sell_src_cd
                when 11 then 'Buick'
                when 12 then 'Cadillac'
                when 13 then 'Chevrolet'
                when 48 then 'GMC'
            end as selling_source,
            f.sell_src_cd,
            f.veh_prod_rpt_model_cd,
            f.deliv_type_ctgy,
            row_number() over 
                (partition by f.veh_id_nbr || f.biz_assoc_id
                order by f.orig_proc_dt asc) row_num    -- identify all process dates for CTP event/vin

        from
            temp_V360 f, rtl_sales_thru


        where
            f.ntl_cd = 'US' --reporting country
           and f.kpi_type_cd in (25,95)
            and f.sell_src_cd in (11,12,13,48) --Buick, Cadillac, Chevrolet, GMC
            and f.orig_proc_dt <= rtl_sales_thru.sales_thru_dt 
           and year(f.orig_proc_dt) >= 2021 
          --and datediff(to_date(rtl_sales_thru.prior_yr_start_dt), f.orig_proc_dt) < 365  -- no records earlier than Jan 2021
          and f.veh_deliv_cnt = 1

        ),  -- pull all CTP records (+1) back to January 2021

    Inc_Dlr_Seq as (
        Select
        veh_id_nbr,
        orig_proc_dt,
        biz_assoc_id,
        model_yr,
        selling_source,
        sell_src_cd,
        veh_prod_rpt_model_cd,
        row_number() over 
                (partition by veh_id_nbr
                order by orig_proc_dt desc) out_dlr_num -- most recent dealer with CTP event for vin 


        from In_Serv_Dtl

        where row_num = 1   -- First process date for Dlr/CTP event 

        ),  --Orders Dlr/VIN events



    Inc_Out_Serv_Dlr_Dtl as (
    Select
        isos.vin,
        outcpt.orig_proc_dt,
        isos.outdate,
        isos.daysinservice,
        isos.todaydate,
        outcpt.biz_assoc_id,
        outcpt.selling_source, 
        outcpt.sell_src_cd,
        outcpt.veh_prod_rpt_model_cd,
        outcpt.model_yr,
        isos.outmile,
        isos.outtype,
        isos.loangreaterthan60,
        row_number() over 
                (partition by vin
                order by todaydate desc) dlr_seq_num    -- number the sequence of events per vin by file date

    from
       Inc_Stg_Out_Serv isos

    left join
        Inc_Dlr_Seq outcpt
        on outcpt.veh_id_nbr = isos.vin

    where isos.todaydate = to_date(current_date) -- Today's file
    and isos.row_num = 1                    -- eliminate duplicate ontrac record and choose most recent record
    and outcpt.out_dlr_num = 1              -- select last dealer/bac with CTP event for vin
    and isos.outdate > outcpt.orig_proc_dt   -- outdate must occur after process date




    )   -- chooses most recent Dlr/CT record

    select
        vin as veh_id_nbr,
        todaydate as file_dt,
        orig_proc_dt as in_svc_dt,
        outdate as out_svc_dt,
        biz_assoc_id as biz_assoc_id,
        daysinservice as day_in_svc_cnt,
        outmile as out_svc_mil_cnt,
        outtype as out_svc_type_cd,
        selling_source as sell_src_desc, 
        sell_src_cd as sell_src_cd,
        veh_prod_rpt_model_cd as veh_prod_rpt_model_cd,
        model_yr as model_yr,
        loangreaterthan60 as loan_great_than_60_day_cd

    From Inc_Out_Serv_Dlr_Dtl  


    group by

        vin,
        todaydate,
        orig_proc_dt,
        outdate,
        biz_assoc_id,
        daysinservice,
        outmile,
        outtype,
        selling_source, 
        sell_src_cd,
        veh_prod_rpt_model_cd,
        model_yr,
        loangreaterthan60

    ---- final column names -----



       -----       INCREMENTAL SQL     -----

     ------  COURTESY TRANSPORTATION CDS  ------

"""



# Main Function
def main(ENV, script_name, detroit_tz, tidal_name, target_table, target_path):
    
    # Start-Spark
    spark = initialize_spark_session(script_name)

    # Define Source-Tables
    RetC_table = "database.ret_crtsy_trp_pgm_stg"
    M360_table = "marketing360_public.fs_gbl_sales_cal_day_rpt_lkp"
    V360_table = "vehicle360.fs_daily_veh_deliv"
    create_temp_views(spark, RetC_table, M360_table, V360_table)

    
    target_schema = [
        "Veh_Id_Nbr",
        "File_Dt",
        "Out_Svc_Dt",
        "Src_Sys_Id",
        "In_Svc_Dt",
        "Biz_Assoc_Id",
        "Day_In_Svc_Cnt",
        "Out_Svc_Mil_Cnt",
        "Sell_Src_Cd",
        "Sell_Src_Desc",
        "Veh_Prod_Rpt_Model_Cd",
        "Model_Yr",
        "Out_Svc_Type_Cd",
        "Loan_Great_Than_60_Day_Cd",
        "Src_Sys_Uniq_Prim_Key_Val",
        "Src_Sys_Uniq_Prim_Key_Col",
        "Meas_Cnt",
        "Dw_Anom_Flg",
        "Dw_Mod_Ts",
        "Dw_Job_Id",
        "Src_Sys_Iud_Cd",
    ]

    try:
        df_historical    = spark.sql(CTP_HISTORICAL_SQL)
        df_incremental   = spark.sql(CTP_INCREMENTAL_SQL)
        
        df_combined      = df_historical.union(df_incremental); info(df_combined)
    
        # Create Plumbing Columns with Detroit timezone
        plumbing_columns = create_plumbing_columns(tidal_name, detroit_tz)

        # Apply Plumbing Columns and Reorder as per target_schema
        df_transformed   = (df_combined
		                  .withColumn("Model_yr", F.col("Model_Yr").cast("int"))
                          .withColumn("Src_Sys_Id", plumbing_columns['SRC_SYS_ID'])
                          .withColumn("Src_Sys_Uniq_Prim_Key_Val", plumbing_columns['SRC_SYS_UNIQ_PRIM_KEY_VAL'])
                          .withColumn("Src_Sys_Uniq_Prim_Key_Col", plumbing_columns['SRC_SYS_UNIQ_PRIM_KEY_COL'])
                          .withColumn("Meas_Cnt", plumbing_columns['MEAS_CNT'])
                          .withColumn("Dw_Anom_Flg", plumbing_columns['DW_ANOM_FLG'])
                          .withColumn("Dw_Mod_Ts", plumbing_columns['CURRENT_TS'])
                          .withColumn("Dw_Job_Id", plumbing_columns['DW_JOB_ID'])
                          .withColumn("Src_Sys_Iud_Cd", plumbing_columns['SRC_SYS_IUD_CD'])
                          .select(*target_schema))

        info(df_transformed)
        

        # Write Final Dataframe to 'target_table'
        COUNT_df_transformed = df_transformed.count()
        print(f"Starting To Write, \033[38;2;0;153;204m'{COUNT_df_transformed}'\033[0m rows from 'df_transformed' to the Target-Table \033[38;2;0;153;204m'{target_table}'\033[0m"); start_write = time.time()
        df_transformed.write.format('parquet').mode('overwrite').saveAsTable(target_table, path=target_path); end_write = time.time(); write_time = end_write - start_write
        print(f"Finished Writing \033[38;2;0;153;204m'{COUNT_df_transformed}'\033[0m rows to the Target-Table, \033[38;2;0;153;204m'{target_table}'\033[0m in \033[38;2;0;153;204m'{write_time}'\033[0m seconds.")

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
    script_name  = "cds_ret_crtsy_trp_pgm_lrf" 
    tidal_name   = "42124-edw-prd-sm-dcfpa-dsr-ret-crtsy-trp-pgm-lrf"
    target_table = f"sales_mktg_processing.{script_name}"
    target_path  = f"/sync/{ENV}_42124_edw_b/EDW/SALES_MKTG/RZ/{script_name}/Processing"

    main(ENV, script_name, detroit_tz, tidal_name, target_table, target_path)
