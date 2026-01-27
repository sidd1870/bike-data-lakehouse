-- Databricks notebook source
CREATE OR REPLACE TABLE gold.dim_customers AS
SELECT DISTINCT
    customer_id,
    first_name,
    last_name,
    marital_status,
    gender,
    created_date
FROM workspace.silver.crm_cust_info;


-- COMMAND ----------

SELECT * FROM gold.dim_customers;


-- COMMAND ----------

SELECT COUNT(*) FROM gold.dim_customers;

