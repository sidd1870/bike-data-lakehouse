# Databricks notebook source
# MAGIC %md
# MAGIC ## **Reading From Bronze Table**
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.bronze.crm_cust_info;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE workspace.bronze.crm_cust_info

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming 

# COMMAND ----------

# MAGIC %sql
# MAGIC REPLACE TABLE workspace.bronze.crm_cust_info AS
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     marital_status,
# MAGIC     gender,
# MAGIC     created_date
# MAGIC FROM workspace.bronze.crm_cust_info;

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Trimming**

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE workspace.bronze.crm_cust_info
# MAGIC SET
# MAGIC     first_name     = TRIM(first_name),
# MAGIC     last_name      = TRIM(last_name),
# MAGIC     marital_status = TRIM(marital_status),
# MAGIC     gender         = TRIM(gender);

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## **Categorical Mapping**

# COMMAND ----------

# MAGIC %sql
# MAGIC REPLACE TABLE workspace.bronze.crm_cust_info AS
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN UPPER(marital_status) = 'S' THEN 'Single'
# MAGIC         WHEN UPPER(marital_status) = 'M' THEN 'Married'
# MAGIC         ELSE 'n/a'
# MAGIC     END AS marital_status,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN UPPER(gender) = 'F' THEN 'Female'
# MAGIC         WHEN UPPER(gender) = 'M' THEN 'Male'
# MAGIC         ELSE 'n/a'
# MAGIC     END AS gender,
# MAGIC
# MAGIC     created_date
# MAGIC FROM workspace.bronze.crm_cust_info;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.bronze.crm_cust_info

# COMMAND ----------

# MAGIC %md
# MAGIC ##Replacing Null **values**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE silver.dim_customers AS
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     COALESCE(first_name, 'Unknown') AS first_name,
# MAGIC     COALESCE(last_name, 'Unknown') AS last_name,
# MAGIC     COALESCE(marital_status, 'n/a') AS marital_status,
# MAGIC     COALESCE(gender, 'n/a') AS gender,
# MAGIC     COALESCE(created_date, '1900-01-01') AS created_date
# MAGIC FROM bronze.crm_cust_info
# MAGIC WHERE customer_id IS NOT NULL; 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.crm_cust_info AS
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     COALESCE(first_name, 'Unknown') AS first_name,
# MAGIC     COALESCE(last_name, 'Unknown') AS last_name,
# MAGIC     COALESCE(marital_status, 'n/a') AS marital_status,
# MAGIC     COALESCE(gender, 'n/a') AS gender,
# MAGIC     COALESCE(created_date, '1900-01-01') AS created_date
# MAGIC FROM bronze.crm_cust_info
# MAGIC WHERE customer_id IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) as total_rows, 
# MAGIC        count(customer_id) as non_null_ids 
# MAGIC FROM bronze.crm_cust_info;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     count_if(customer_id IS NULL) AS null_customer_id,
# MAGIC     count_if(first_name IS NULL) AS null_first_name,
# MAGIC     count_if(last_name IS NULL) AS null_last_name,
# MAGIC     count_if(marital_status IS NULL) AS null_marital_status,
# MAGIC     count_if(gender IS NULL) AS null_gender,
# MAGIC     count_if(created_date IS NULL) AS null_created_date
# MAGIC FROM bronze.crm_cust_info;
# MAGIC FROM bronze.crm_cust_info;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.crm_cust_info

# COMMAND ----------

# MAGIC %md
# MAGIC ##Duplicates check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COALESCE(SUM(duplicate_count), 0) AS duplicate_count
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         customer_id,
# MAGIC         COUNT(*) AS duplicate_count
# MAGIC     FROM bronze.crm_cust_info
# MAGIC     GROUP BY customer_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC ) t;
