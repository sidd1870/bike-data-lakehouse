-- Databricks notebook source
CREATE OR REPLACE TABLE gold.fact_sales AS
SELECT
    order_number,
    customer_id,
    product_key,
    order_date,
    shipping_date,
    due_date,
    sales_amount,
    quantity,
    price
FROM (
    SELECT
        s.order_number,
        s.customer_id,
        s.product_key,
        s.order_date,
        s.shipping_date,
        s.due_date,
        s.sales_amount,
        s.quantity,
        s.price,
        ROW_NUMBER() OVER (
            PARTITION BY s.order_number, s.product_key
            ORDER BY s.order_date
        ) AS rn
    FROM workspace.silver.crm_sales_details s
)
WHERE rn = 1;

-- COMMAND ----------

SELECT * FROM gold.fact_sales

-- COMMAND ----------

SELECT COUNT(*) FROM gold.fact_sales;


-- COMMAND ----------

SELECT order_number, product_key, COUNT(*)
FROM gold.fact_sales
GROUP BY order_number, product_key
HAVING COUNT(*) > 1;


-- COMMAND ----------

SELECT
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customers,
    SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) AS null_products
FROM gold.fact_sales;

-- COMMAND ----------

SELECT * FROM gold.fact_sales

-- COMMAND ----------

SELECT order_number, product_key, COUNT(*)
FROM gold.fact_sales
GROUP BY order_number, product_key
HAVING COUNT(*) > 1;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Total sales by product

-- COMMAND ----------

SELECT
    product_key,
    SUM(sales_amount) AS total_sales
FROM gold.fact_sales
GROUP BY product_key
ORDER BY total_sales DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Sales trend by date

-- COMMAND ----------

SELECT
    date_format(
        try_to_date(CAST(order_date AS STRING), 'yyyyMMdd'),
        'yyyy-MM-dd'
    ) AS order_date,
    SUM(sales_amount) AS daily_sales
FROM gold.fact_sales
WHERE try_to_date(CAST(order_date AS STRING), 'yyyyMMdd') IS NOT NULL
GROUP BY order_date
ORDER BY order_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Total sales by customer **gender**

-- COMMAND ----------

SELECT
    c.gender,
    SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_customers c
    ON f.customer_id = c.customer_id
GROUP BY c.gender;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Top 5 products per customer

-- COMMAND ----------

SELECT
    customer_id,
    SUM(sales_amount) AS total_sales
FROM gold.fact_sales
GROUP BY customer_id
ORDER BY total_sales DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

SELECT *
FROM gold.fact_sales
WHERE quantity > 5
ORDER BY quantity DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Top 10 Products by **Revenue**

-- COMMAND ----------

SELECT
  product_key,
  SUM(sales_amount) AS revenue
FROM gold.fact_sales
GROUP BY product_key
ORDER BY revenue DESC
LIMIT 10;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Total Sales by Product **Name**

-- COMMAND ----------

SELECT
  p.product_name AS Product_Name,
  SUM(f.sales_amount) AS Total_Sales
FROM
  workspace.default.fact_sales f
    JOIN workspace.default.dim_products p
      ON f.product_key = p.product_key
WHERE
  p.product_name IS NOT NULL
  AND f.sales_amount IS NOT NULL
GROUP BY
  p.product_name
ORDER BY
  Total_Sales DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Total Sales per month

-- COMMAND ----------

SELECT 
    DATE_FORMAT(try_to_date(CAST(order_date AS STRING), 'yyyyMMdd'), 'MMM yyyy') AS month,
    SUM(sales_amount) AS total_sales
FROM gold.fact_sales
WHERE try_to_date(CAST(order_date AS STRING), 'yyyyMMdd') IS NOT NULL
GROUP BY DATE_FORMAT(try_to_date(CAST(order_date AS STRING), 'yyyyMMdd'), 'MMM yyyy')
ORDER BY month;

-- COMMAND ----------

SELECT COUNT(*) FROM gold.fact_sales;
