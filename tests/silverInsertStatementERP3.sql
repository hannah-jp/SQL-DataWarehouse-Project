INSERT INTO silver.erp_px_cat_g1v2
(id, cat, subcat, maintenance)

SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2


-- the id must be joinable with cat_id colum from prd_info
SELECT * FROM silver.crm_prd_info
-- same format, so no change

-- check for unwanted spaces
SELECT *
from bronze.erp_px_cat_g1v2
where cat != TRIM(CAT) OR subcat != TRIM(subcat) OR
maintenance != TRIM(maintenance) 
-- NO RESULTS!! GOOD!

-- data standardization for all columns, they all have low cardinality
SELECT DISTINCT maintenance
from bronze.erp_px_cat_g1v2


SELECT * FROM silver.erp_px_cat_g1v2


