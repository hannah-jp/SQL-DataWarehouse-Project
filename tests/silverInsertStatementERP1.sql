INSERT INTO silver.erp_cust_az12(cid, bdate, gen)

SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	 ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
	 ELSE bdate
END AS bdate,
CASE WHEN TRIM(UPPER(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN TRIM(UPPER(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12





-- REMOVING THE NAS and checking if any not there
WHERE cid LIKE '%AW00011000'


SELECT 
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	 ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	 ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info) 


-- CHECKING OUT OF RANGE DATES
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' or bdate > GETDATE()
-- FIX AS ABOVE

-- check cardinality
SELECT DISTINCT gen
FROM bronze.erp_cust_az12