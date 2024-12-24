-- MTD Sales from Start of Month to Current Date
WITH lookup_data AS (
    SELECT
        salesthroughdate AS sell_dt,
        currentmonthmthstart AS curr_mth_start_dt
    FROM
        marketing360_public.fs_vsna_dim_selling_day_report_lkup
),
fleet_oos_with_lookup AS (
    SELECT
        fleet_oos.*,
        lookup_data.curr_mth_start_dt
    FROM
        fleet_oos
    JOIN
        lookup_data ON fleet_oos.rpt_dt = lookup_data.sell_dt
),
agg_months AS (
    SELECT
        rpt_dt,
        curr_mth_start_dt,
        SUM(jan) as jan_total,
        SUM(feb) as feb_total,
        SUM(mar) as mar_total,
        ...
    FROM
        fleet_oos_with_lookup
    GROUP BY
        rpt_dt, curr_mth_start_dt
),
mtd_sales AS (
    SELECT
        rpt_dt,
        CASE
            WHEN EXTRACT(MONTH FROM curr_mth_start_dt) = 1 THEN jan_total
            WHEN EXTRACT(MONTH FROM curr_mth_start_dt) = 2 THEN feb_total
            ...
        END AS mtd_sales
    FROM
        agg_months
),
requested_date AS (
    SELECT
        '2023-03-11' AS requested_rpt_dt, -- Store the original requested date
        rpt_dt,
        mtd_sales
    FROM
        mtd_sales
    WHERE
        rpt_dt <= '2023-03-11' -- Get all rows with rpt_dt less than or equal to the specified date
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
