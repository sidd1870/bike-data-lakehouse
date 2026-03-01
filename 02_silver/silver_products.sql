-- Databricks notebook source
-- MAGIC %md
-- MAGIC %md
-- MAGIC ## **Reading From Bronze Table**
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------


-- COMMAND ----------

SELECT * FROM bronze.crm_prd_info

-- COMMAND ----------



-- COMMAND ----------

DROP TABLE IF EXISTS silver.crm_prd_info

-- COMMAND ----------

DESCRIBE bronze.crm_prd_info;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Renaming

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.crm_prd_info AS
SELECT
    prd_key                       AS product_key,
    TRIM(prd_nm)                  AS product_name,
    UPPER(TRIM(prd_line))         AS product_line,
    prd_cost                      AS product_cost,
    prd_start_dt                  AS start_date,
    prd_end_dt                    AS end_date
FROM bronze.crm_prd_info;

-- COMMAND ----------



-- MAGIC %md
--##Trimming
-- MAGIC ##Data standardization
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.crm_prd_info AS
SELECT
    prd_id AS product_id,
    TRIM(prd_key) AS product_key,
    TRIM(prd_nm) AS product_name,
    prd_cost AS product_cost,
    CASE
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'SPORT'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'ROAD'
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'MOUNTAIN'
        ELSE 'UNKNOWN'
    END AS product_line,
    prd_start_dt AS start_date,
    prd_end_dt AS end_date
FROM bronze.crm_prd_info;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Null value standardization

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.crm_prd_info AS
SELECT
    prd_id AS product_id,
    TRIM(prd_key) AS product_key,
    TRIM(prd_nm) AS product_name,
    COALESCE(prd_cost, 0) AS product_cost,
    CASE
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'SPORT'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'ROAD'
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'MOUNTAIN'
        ELSE 'UNKNOWN'
    END AS product_line,
    prd_start_dt AS start_date,
    COALESCE(prd_end_dt, '2099-12-31') AS end_date 
FROM bronze.crm_prd_info;

-- COMMAND ----------

SELECT COUNT(*) FROM bronze.crm_prd_info ;


-- COMMAND ----------

SELECT COUNT(*) FROM silver.crm_prd_info;


-- COMMAND ----------



-- COMMAND ----------

SELECT * FROM silver.crm_prd_info

-- COMMAND ----------

SELECT
  SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_id,
  SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) AS null_product_key,
  SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) AS null_product_name,
  SUM(CASE WHEN product_cost IS NULL THEN 1 ELSE 0 END) AS null_product_cost,
  SUM(CASE WHEN product_line IS NULL THEN 1 ELSE 0 END) AS null_product_line,
  SUM(CASE WHEN start_date IS NULL THEN 1 ELSE 0 END) AS null_start_date,
  SUM(CASE WHEN end_date IS NULL THEN 1 ELSE 0 END) AS null_end_date
FROM silver.crm_prd_info;

-- COMMAND ----------

SELECT
  product_id,
  product_key,
  product_name,
  product_cost,
  product_line,
  start_date,
  end_date,
  COUNT(*) AS duplicate_count
FROM silver.crm_prd_info
GROUP BY
  product_id,
  product_key,
  product_name,
  product_cost,
  product_line,
  start_date,
  end_date
HAVING COUNT(*) > 1
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
