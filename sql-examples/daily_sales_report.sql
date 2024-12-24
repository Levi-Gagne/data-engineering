-- Daily Sales Calculation
WITH rtl_sales_thru AS (
    SELECT
        salesthroughdate AS sell_dt,
        currentmonthmthstart AS cur_mth_start_dt
    FROM
        marketing360_public.fs_vsna_dim_selling_day_report_lkup
),
daily_sales AS (
    SELECT
        SUM(daily_sales_cnt) AS daily_sales_fleet_oos
    FROM
        fleet_oos_1
    WHERE
        model_nm = 'Fleet_OOS' AND
        rpt_dt = '2023-01-01' -- Replace this date with the desired date
)
SELECT
    daily_sales.daily_sales_fleet_oos
FROM
    daily_sales;
