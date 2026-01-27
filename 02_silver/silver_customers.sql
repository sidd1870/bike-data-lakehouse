
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
# MAGIC     cst_id             AS customer_id,
# MAGIC     cst_key            AS customer_key,
# MAGIC     cst_firstname      AS first_name,
# MAGIC     cst_lastname       AS last_name,
# MAGIC     cst_marital_status AS marital_status,
# MAGIC     cst_gndr           AS gender,
# MAGIC     cst_create_date    AS created_date
# MAGIC FROM workspace.bronze.crm_cust_info;

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Trimming**

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE workspace.bronze.crm_cust_info
# MAGIC SET
# MAGIC     customer_id    = TRIM(customer_id),
# MAGIC     customer_key   = TRIM(customer_key),
# MAGIC     first_name     = TRIM(first_name),
# MAGIC     last_name      = TRIM(last_name),
# MAGIC     marital_status = TRIM(marital_status),
# MAGIC     gender         = TRIM(gender);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## **Normalization**

# COMMAND ----------

# MAGIC %sql
# MAGIC  REPLACE TABLE workspace.bronze.crm_cust_info AS
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_key,
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
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.bronze.crm_cust_info

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     marital_status, 
# MAGIC     gender, 
# MAGIC     COUNT(*) as record_count
# MAGIC FROM workspace.bronze.crm_cust_info
# MAGIC GROUP BY 1, 2;
