-- Databricks notebook source
CREATE OR REPLACE TABLE gold.dim_products AS
SELECT DISTINCT
    product_key,
    product_name,
    product_line,
    product_cost,
    start_date,
    end_date
FROM silver.crm_prd_info;



-- COMMAND ----------

SELECT COUNT(*) FROM gold.dim_products;


-- COMMAND ----------

SELECT DISTINCT product_key
FROM gold.fact_sales
LIMIT 20;


-- COMMAND ----------

SELECT COUNT(*)
FROM gold.fact_sales f
JOIN gold.dim_products_current p
  ON f.product_key = p.product_key;

