-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## **Reading From Bronze Table**

-- COMMAND ----------

SELECT * FROM workspace.bronze.crm_cust_info;

-- COMMAND ----------

DESCRIBE TABLE workspace.bronze.crm_cust_info;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Renaming 

-- COMMAND ----------

REPLACE TABLE workspace.bronze.crm_cust_info AS
SELECT 
     cst_id             AS customer_id,
     cst_key            AS customer_key,
     cst_firstname      AS first_name,
     cst_lastname       AS last_name,
     cst_marital_status AS marital_status,
     cst_gndr           AS gender,
     cst_create_date    AS created_date
FROM workspace.bronze.crm_cust_info;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## **Trimming**

-- COMMAND ----------

UPDATE workspace.bronze.crm_cust_info
SET
     customer_id    = TRIM(customer_id),
     customer_key   = TRIM(customer_key),
     first_name     = TRIM(first_name),
     last_name      = TRIM(last_name),
     marital_status = TRIM(marital_status),
     gender         = TRIM(gender);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## **Normalization**

-- COMMAND ----------

REPLACE TABLE workspace.bronze.crm_cust_info AS
SELECT
     customer_id,
     customer_key,
     first_name,
     last_name,
     CASE
         WHEN UPPER(marital_status) = 'S' THEN 'Single'
         WHEN UPPER(marital_status) = 'M' THEN 'Married'
         ELSE 'n/a'
     END AS marital_status,
     CASE
         WHEN UPPER(gender) = 'F' THEN 'Female'
         WHEN UPPER(gender) = 'M' THEN 'Male'
         ELSE 'n/a'
     END AS gender,
     created_date
FROM workspace.bronze.crm_cust_info;

-- COMMAND ----------

SELECT * FROM workspace.bronze.crm_cust_info;

-- COMMAND ----------

SELECT 
     marital_status, 
     gender, 
     COUNT(*) as record_count
FROM workspace.bronze.crm_cust_info
GROUP BY 1, 2;
