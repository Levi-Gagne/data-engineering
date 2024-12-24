-- Prior Month MTD Sales
-- This query calculates the month-to-date (MTD) sales up to a specific requested date
-- within the prior month, summing sales by day for different products.

WITH requested_date_constant AS (
    SELECT '2023-01-04' AS requested_date -- Define the requested date here
),
lookup_data AS (
    SELECT
        salesthroughdate AS sell_dt,
        priormthcurrentday AS prior_mth_current_day
    FROM
        marketing360_public.fs_vsna_dim_selling_day_report_lkup
),
fleet_oos_with_lookup AS (
    SELECT
        fleet_oos.*,
        lookup_data.prior_mth_current_day
    FROM
        fleet_oos
    JOIN
        lookup_data ON fleet_oos.sales_through_dt = lookup_data.sell_dt
),
agg_months AS (
    SELECT
        rpt_dt,
        prior_mth_current_day,
        SUM(jan) as jan_total,
        SUM(feb) as feb_total,
        ...
        SUM(dec) as dec_total
    FROM
        fleet_oos_with_lookup
    GROUP BY
        rpt_dt, prior_mth_current_day
),
mtd_sales AS (
    SELECT
        rpt_dt,
        SUM(CASE
            WHEN MONTH(prior_mth_current_day) = 1 THEN jan_total
            WHEN MONTH(prior_mth_current_day) = 2 THEN feb_total
            ...
        END) AS mtd_sales
    FROM
        agg_months
    GROUP BY
        rpt_dt
),
requested_date AS (
    SELECT
        requested_date_constant.requested_date AS requested_rpt_dt,
        rpt_dt,
        mtd_sales
    FROM
        mtd_sales,
        requested_date_constant
    WHERE
        rpt_dt <= requested_date_constant.requested_date
)
SELECT
    requested_rpt_dt,
    mtd_sales
FROM
    requested_date
WHERE
    mtd_sales IS NOT NULL
ORDER BY
    rpt_dt DESC
LIMIT 1;
