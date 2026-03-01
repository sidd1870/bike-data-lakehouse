-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## **Reading From Bronze Table**

-- COMMAND ----------

SELECT * FROM bronze.crm_sales_details

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Data **transformation**

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.crm_sales_details AS
SELECT
    sls_ord_num AS order_number,
    sls_prd_key AS product_key,
    sls_cust_id AS customer_id,
    
    try_to_date(cast(sls_order_dt AS STRING), 'yyyyMMdd') AS order_date,
    try_to_date(cast(sls_ship_dt AS STRING), 'yyyyMMdd') AS shipping_date,
    try_to_date(cast(sls_due_dt AS STRING), 'yyyyMMdd') AS due_date,
    sls_sales AS sales_amount,
    sls_quantity AS quantity,
    sls_price AS price
FROM bronze.crm_sales_details;

-- COMMAND ----------

SELECT * FROM silver.crm_sales_details

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##**Replacing** **Nulls**

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.crm_sales_details AS
SELECT
    COALESCE(sls_ord_num, 'UNKNOWN') AS order_number,
    COALESCE(sls_prd_key, 'UNKNOWN') AS product_key,
    COALESCE(sls_cust_id, 'UNKNOWN') AS customer_id,
    COALESCE(try_to_date(cast(sls_order_dt AS STRING), 'yyyyMMdd'), DATE('1900-01-01')) AS order_date,
    COALESCE(try_to_date(cast(sls_ship_dt AS STRING), 'yyyyMMdd'), DATE('1900-01-01')) AS shipping_date,
    COALESCE(try_to_date(cast(sls_due_dt AS STRING), 'yyyyMMdd'), DATE('1900-01-01')) AS due_date,
    COALESCE(sls_sales, 0) AS sales_amount,
    COALESCE(sls_quantity, 0) AS quantity,
    COALESCE(sls_price, 0) AS price
FROM bronze.crm_sales_details
WHERE
    sls_ord_num IS NOT NULL
    AND sls_prd_key IS NOT NULL
    AND sls_cust_id IS NOT NULL;

-- COMMAND ----------

SELECT
    COUNT(*) AS total_rows,
    COUNT_IF(order_number IS NULL) AS null_order_number,
    COUNT_IF(product_key IS NULL) AS null_product_key,
    COUNT_IF(customer_id IS NULL) AS null_customer_id,
    COUNT_IF(order_date IS NULL) AS null_order_date,
    COUNT_IF(shipping_date IS NULL) AS null_shipping_date,
    COUNT_IF(due_date IS NULL) AS null_due_date,
    COUNT_IF(sales_amount IS NULL) AS null_sales_amount,
    COUNT_IF(quantity IS NULL) AS null_quantity,
    COUNT_IF(price IS NULL) AS null_price
FROM silver.crm_sales_details

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Duplicates **check**

-- COMMAND ----------

SELECT
    order_number,
    product_key,
    customer_id,
    COUNT(*) AS duplicate_count
FROM silver.crm_sales_details
GROUP BY order_number, product_key, customer_id
HAVING COUNT(*) > 1

UNION ALL

SELECT
    NULL AS order_number,
    NULL AS product_key,
    NULL AS customer_id,
    0 AS duplicate_count
WHERE NOT EXISTS (
    SELECT 1
    FROM silver.crm_sales_details
    GROUP BY order_number, product_key, customer_id
    HAVING COUNT(*) > 1
)
