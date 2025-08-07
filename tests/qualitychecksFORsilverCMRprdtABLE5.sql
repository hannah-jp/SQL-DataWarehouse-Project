--Checks for NULLS or Duplicates in Primary Key
-- Expectation: No Results

SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL
-- No NULLS in the primary key above, GOOD

-- check unwanted spaces
SELECT
prd_nm
FROM silver.crm_prd_info
WHERE  prd_nm != TRIM(prd_nm)


-- Check quality of numbers - nulls or negatives
SELECT prd_cost
FROM silver.crm_prd_info
WHERE  prd_cost < 0 OR prd_cost IS NULL
-- no negatives but 2 NULLS, So we can replace the nullS
-- with 0 is business allows it
-- ISNULL allows us to replace with the the value we want
-- can use COALESCE as well

-- Data Standardization and Consistency
-- Low cardinality in this column so check possible values
SELECT DISTINCT prd_line
FROM silver.crm_prd_info
-- replace with full names

SELECT *
FROM silver.crm_prd_info