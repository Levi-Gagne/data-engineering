/*
  Title: Brand-Prioritized Sorting for Reporting
  Description: This SQL script is designed to order sales data based on the source brand in a manner 
               optimized for clarity and reporting efficiency. The primary focus is on categorizing 
               and sorting vehicle sales by manufacturer and model. Special conditions are used 
               to handle aggregated total rows for better data segmentation and analysis. 
               This script is particularly useful for creating structured reports in BI tools like Power BI,
               where data needs to be displayed in a logically sorted order.

  Use case: Intended for use in the automotive industry where sales data are segmented by brand and model. 
            This script assists in generating clear, sorted reports, aiding in the comprehension and analysis 
            of sales performance across different car brands and models.
            
  Created by: Levi Gagne
  Date: 2024-08-01
*/

-- Start by ordering the results primarily by the 'selling_source'
ORDER BY
  -- Prioritize the order of car brands; assign each a numeric value for sorting
  CASE 
    WHEN selling_source = 'Buick' THEN 1  -- Buick is prioritized as 1
    WHEN selling_source = 'GMC' THEN 2    -- GMC comes after Buick
    WHEN selling_source = 'Chevrolet' THEN 3 -- Chevrolet follows GMC
    WHEN selling_source = 'Cadillac' THEN 4 -- Cadillac is next
    ELSE 5  -- Any other selling source not listed comes last
  END,
  selling_source, -- Then sort alphabetically by the selling source if there are ties in the CASE above

  -- Handle special total rows for rpt_model_name_1 which includes aggregate totals for brands
  CASE
    WHEN rpt_model_name_1 IN ('Buick Total', 'GMC Total', 'Chevrolet Total', 'Cadillac Total') THEN 2
    ELSE 1 -- Regular models are prioritized over total aggregates unless specified
  END,
  
  -- Extract and order by the first word of the model name for further sorting clarity
  LEFT(rpt_model_name_1, CHARINDEX(' ', rpt_model_name_1 + ' ') - 1),

  -- Special sorting case for generic totals not included in the main brands
  CASE
    WHEN rpt_model_name_1 LIKE '% Total' AND rpt_model_name_1 NOT IN ('Buick Total', 'GMC Total', 'Chevrolet Total', 'Cadillac Total') THEN 2
    ELSE 1 -- Non-total models are given priority in the sorting
  END,

  rpt_model_name_1 -- Finally, sort by the model name itself for any remaining ties
