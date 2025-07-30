-- Returns Lookup Value for Specific Reporting Dates
WITH
staging_sales AS (
    SELECT
        rpt_dt,
        sales_through_dt
    FROM
        fleet_oos
    WHERE
        rpt_dt = '2023-03-10'
),
lookup_data AS (
    SELECT
        salesthroughdate AS sell_dt,
        priormthend AS prior_mth_end_date
    FROM
        marketing360_public.fs_vsna_dim_selling_day_report_lkup
),
matched_lookup_data AS (
    SELECT
        staging_sales.rpt_dt,
        staging_sales.sales_through_dt,
        lookup_data.prior_mth_end_date
    FROM
        staging_sales
    JOIN
        lookup_data ON staging_sales.sales_through_dt = lookup_data.sell_dt
)
SELECT
    rpt_dt,
    prior_mth_end_date
FROM
    matched_lookup_data;
