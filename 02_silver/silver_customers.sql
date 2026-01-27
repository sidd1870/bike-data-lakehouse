-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## **Reading From Bronze Table**

-- COMMAND ----------

SELECT * FROM bronze.crm_sales_details

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Data type standardization

-- COMMAND ----------

SELECT
    TRIM(sls_ord_num)                  AS order_number,
    TRIM(sls_prd_key)                  AS product_key,
    CAST(sls_cust_id AS INT)           AS customer_id,

    TO_DATE(CAST(sls_order_dt AS STRING), 'yyyyMMdd') AS order_date,
    TO_DATE(CAST(sls_ship_dt  AS STRING), 'yyyyMMdd') AS ship_date,
    TO_DATE(CAST(sls_due_dt   AS STRING), 'yyyyMMdd') AS due_date,

    CAST(sls_sales   AS DECIMAL(10,2)) AS sales_amount,
    CAST(sls_quantity AS INT)          AS quantity,
    CAST(sls_price   AS DECIMAL(10,2)) AS unit_price
FROM bronze.crm_sales_details;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Replacing
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.crm_sales_details AS
SELECT
    order_number,
    customer_id,
    product_key,
    order_date,
    ship_date,
    due_date,
    sales_amount,
    quantity,
    unit_price
FROM bronze.crm_sales_details;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##NULL Handling

-- COMMAND ----------

SELECT *
FROM (
    SELECT
        TRIM(sls_ord_num) AS order_number,
        TRIM(sls_prd_key) AS product_key,
        CAST(sls_cust_id AS INT) AS customer_id,
        TO_DATE(CAST(sls_order_dt AS STRING), 'yyyyMMdd') AS order_date,
        TO_DATE(CAST(sls_ship_dt  AS STRING), 'yyyyMMdd') AS ship_date,
        TO_DATE(CAST(sls_due_dt   AS STRING), 'yyyyMMdd') AS due_date,
        CAST(sls_sales AS DECIMAL(10,2)) AS sales_amount,
        CAST(sls_quantity AS INT) AS quantity,
        CAST(sls_price AS DECIMAL(10,2)) AS unit_price
    FROM bronze.crm_sales_details
)
WHERE order_number IS NOT NULL
  AND product_key IS NOT NULL
  AND customer_id IS NOT NULL
  AND order_date IS NOT NULL
  AND quantity > 0
  AND unit_price >= 0
  AND sales_amount IS NOT NULL;

-- COMMAND ----------

SELECT * FROM bronze.crm_sales_details
