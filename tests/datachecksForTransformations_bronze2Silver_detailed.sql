/*
This script is checking each column and doing the transformation after the error is identified.
- Do not run this script at once, it is more so for understanding.
- The clean script is in proc_silver
*/

-- Check for NULLS or Duplicates in Primary Key
-- Expectation: NO Result

-- Doing an aggregate on Primary Key
SELECT  cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 or cst_id IS NULL
-- 5 IDs occur more than once and a null exists

-- transformation
SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466 
-- Found 3 times, according to the date, pick the newest one
-- Use window function to partition by cust_id and RANK

SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
WHERE cst_id = 29466

SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info

SELECT * 
FROM (
SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last != 1
-- The above results are the ones that old and causing duplicated in primary key
-- So make the flag_last = 1 so that only the primary key is unique and exists only once, below

SELECT * 
FROM (
SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last = 1

-- =================================================================
-- Check for unwanted spaces

-- If the original value is not equal to the same value after trimming, it means there are spaces!
-- Expectation here: NO RESULTS
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_key
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key)

-- transformation

-- add column names in select instead of *, and then add TRIM
SELECT cst_id, 
cst_key, 
cst_firstname, 
cst_lastname, 
cst_marital_status,
cst_gndr,
cst_create_date
FROM (
SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last = 1

SELECT cst_id, 
cst_key, 
TRIM(cst_firstname) AS cst_firstname, 
TRIM(cst_lastname) AS cst_lastname, 
cst_marital_status,
cst_gndr,
cst_create_date
FROM (
SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last = 1

-- ==========================================================
-- Quality check - Check the consistency of values in low cardinality columns
-- low cardinality means limited number of possible values
-- such columns here are marital status and gender

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info
-- If we make a rule that we aim to store clear and meaningful values rather than abbreviated terms
-- we need to map these values to full words
-- Making a case in the select, all nulls are n/a
-- If the m or f in gender are lower case, making them upper with UPPER
-- If want to make sure no white spaces, TRIM gndr

SELECT cst_id, 
cst_key, 
TRIM(cst_firstname) AS cst_firstname, 
TRIM(cst_lastname) AS cst_lastname, 
cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last = 1

-- DO the same for Marital status
SELECT cst_id, 
cst_key, 
TRIM(cst_firstname) AS cst_firstname, 
TRIM(cst_lastname) AS cst_lastname, 
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last = 1

-- ==========================================
-- Date -  we got to make sure it is a real date type and not a string or varchar
-- from the data type as we defined in the data type, we know it is right