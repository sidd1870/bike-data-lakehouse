-- Databricks notebook source
CREATE OR REPLACE TABLE gold.dim_products AS
SELECT
    product_key,
    product_name,
    product_line,
    start_date,
    end_date
FROM (
    SELECT
        p.*,
        ROW_NUMBER() OVER (
            PARTITION BY product_key
            ORDER BY end_date DESC NULLS LAST, start_date DESC
        ) AS rn
    FROM workspace.silver.crm_prd_info p
)
WHERE rn = 1;



-- COMMAND ----------

SELECT COUNT(*) FROM gold.dim_products;


-- COMMAND ----------

SELECT DISTINCT product_key
FROM gold.fact_sales
LIMIT 20;


-

