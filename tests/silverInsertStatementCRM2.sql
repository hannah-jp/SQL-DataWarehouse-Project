INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)

SELECT
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	 WHEN 'M' THEN 'Mountain'
	 WHEN 'R' THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'T' THEN 'Touring'
	 ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info

SELECT * 
FROM silver.crm_prd_info



-- Make sure we can join data together for cat_id
SELECT DISTINCT id from bronze.erp_px_cat_g1v2
-- There are underscores in this table, so replace - with _ in our new column
-- also check not in - WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN
--	(SELECT DISTINCT id from bronze.erp_px_cat_g1v2)

-- ----------
-- FOR THE 2ND column we created, checking join compatibility with following
SELECT sls_prd_key FROM bronze.crm_sales_details
-- ALSO CHECK NOT IN & IN - WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN
--	(SELECT sls_prd_key FROM bronze.crm_sales_details)


-- Checking the prd_start date and end date
-- the end date is older than start date - NOT RIGHT
-- IN SQL, if we want to access another info from another record, we have
-- LEAD() and LAG() 
-- Our scenario, we need to access the next record, so lead()
-- ADD -1 to get the day before
-- ALso CAST THE DATES to be only Dates and not DATETIME because time is always 00:00
SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')
