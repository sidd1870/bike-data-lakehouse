-- Databricks notebook source
-- MAGIC %md
-- MAGIC %md
-- MAGIC ## **Reading From Bronze Table**
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

SELECT * FROM bronze.crm_prd_info

-- COMMAND ----------

DROP TABLE IF EXISTS silver.crm_prd_info

-- COMMAND ----------

DESCRIBE bronze.crm_prd_info;


-- COMMAND ----------

CREATE OR REPLACE TABLE silver.crm_prd_info AS
SELECT
    product_key,
    TRIM(product_name)              AS product_name,
    UPPER(TRIM(product_line))       AS product_line,
    product_cost,
    start_date,
    end_date
FROM bronze.crm_prd_info;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Trimming

-- COMMAND ----------

SELECT
  TRIM(UPPER(product_key)) AS product_key,
  product_name,
  product_line,
  product_cost,
  start_date,
  end_date
FROM bronze.crm_prd_info;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Normalization
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.crm_prd_info AS
SELECT
    product_id,
    TRIM(product_key) AS product_key,
    TRIM(product_name) AS product_name,
    product_cost,
    CASE
        WHEN UPPER(TRIM(product_line)) = 'S' THEN 'SPORT'
        WHEN UPPER(TRIM(product_line)) = 'R' THEN 'ROAD'
        WHEN UPPER(TRIM(product_line)) = 'M' THEN 'MOUNTAIN'
        ELSE 'UNKNOWN'
    END AS product_line,
    start_date,
    end_date
FROM bronze.crm_prd_info;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Null value standardization

-- COMMAND ----------

SELECT
    product_id,
    product_key,
    product_name,
    COALESCE(product_cost, 0) AS product_cost,
    product_line,
    start_date,
    end_date
FROM bronze.crm_prd_info;

-- COMMAND ----------

SELECT COUNT(*) FROM bronze.crm_prd_info ;


-- COMMAND ----------

SELECT COUNT(*) FROM silver.crm_prd_info;


-- COMMAND ----------

SELECT * FROM silver.crm_prd_info
