-- Read bronze table
SELECT *
FROM bronze.crm_cust_info;

-- Rename / standardize columns (if needed)
CREATE OR REPLACE TABLE bronze.crm_cust_info AS
SELECT 
    customer_id,
    first_name,
    last_name,
    marital_status,
    gender,
    created_date
FROM bronze.crm_cust_info;

-- Trim string fields
UPDATE bronze.crm_cust_info
SET
    first_name     = TRIM(first_name),
    last_name      = TRIM(last_name),
    marital_status = TRIM(marital_status),
    gender         = TRIM(gender);

-- Categorical mapping
CREATE OR REPLACE TABLE bronze.crm_cust_info AS
SELECT
    customer_id,
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
FROM bronze.crm_cust_info;

-- Create Silver dimension table with null handling
CREATE OR REPLACE TABLE silver.dim_customers AS
SELECT 
    customer_id,
    COALESCE(first_name, 'Unknown') AS first_name,
    COALESCE(last_name, 'Unknown') AS last_name,
    COALESCE(marital_status, 'n/a') AS marital_status,
    COALESCE(gender, 'n/a') AS gender,
    COALESCE(created_date, DATE '1900-01-01') AS created_date
FROM bronze.crm_cust_info
WHERE customer_id IS NOT NULL;

-- Data quality checks
SELECT COUNT(*) AS total_rows,
       COUNT(customer_id) AS non_null_ids
FROM bronze.crm_cust_info;

SELECT
    COUNT_IF(customer_id IS NULL) AS null_customer_id,
    COUNT_IF(first_name IS NULL) AS null_first_name,
    COUNT_IF(last_name IS NULL) AS null_last_name,
    COUNT_IF(marital_status IS NULL) AS null_marital_status,
    COUNT_IF(gender IS NULL) AS null_gender,
    COUNT_IF(created_date IS NULL) AS null_created_date
FROM bronze.crm_cust_info;

-- Duplicate check
SELECT 
    COALESCE(SUM(duplicate_count), 0) AS duplicate_count
FROM (
    SELECT 
        customer_id,
        COUNT(*) AS duplicate_count
    FROM bronze.crm_cust_info
    GROUP BY customer_id
    HAVING COUNT(*) > 1
) t;
