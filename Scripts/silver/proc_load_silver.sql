/*
===============================================================
Stored Procedure: Load Silver Layer(Bronze -> Silver)
===============================================================
Script Purpose:
     This stored procedure performs the ETL(Extract, Transform, Load) proocess to
     populate the silver schema tables from the bronze schema

Actions Performed:
     - Truncates Silver tables.
     - Inserts transformed and cleaned data from bronze into silver tables.

Parameters:
     - None.
     - This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC Silver.load_Silver;
       
*/


-- Creating store procedure
CREATE OR ALTER PROCEDURE Silver.load_Silver
AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
BEGIN TRY
      SET @batch_start_time = GETDATE();
      PRINT '========================================';
      PRINT 'Loading Silver Layer';
      PRINT '========================================';

      PRINT'-----------------------------------------'
      PRINT'Loading CRM Tables';
      PRINT'-----------------------------------------'
      SET @start_time = GETDATE();
-- Inserting Data in Silver tables
PRINT'>> Truncating Table: Silver.crm_cust_info';
TRUNCATE TABLE Silver.crm_cust_info;
PRINT'>> Inserting Data Into: crm_cust_info';
INSERT INTO Silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date

)

SELECT --Query to remove the duplicates/ remove spaces
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM(
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM Bronze.crm_cust_info
)t WHERE flag_last = 1;
SET @end_time = GETDATE();
PRINT'>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +'seconds'
PRINT'>> ------------';

-- Check For unwanted spaces
-- Expectation No result
SELECT cst_firstname
FROM Silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


SELECT cst_lastname
FROM Silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM Silver.crm_cust_info


SELECT DISTINCT cst_marital_status
FROM Silver.crm_cust_info


-- **************************************
-- Silvecrm_prd_info
-- **************************************

-- For crm_prd_info
-- Checking for nulls or duplicates in primary key
-- Expectation: No Result
SELECT
prd_id,
COUNT(*)
FROM Bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

--Check for unwanted spaces
--Expectation: No Results
SELECT prd_nm
FROM Bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

--Check For nulls or negative numbers
--Expectation: No Results
SELECT prd_nm
FROM Bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)


SELECT prd_cost
FROM Bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM Bronze.crm_prd_info

--Check For invalid Date Orders
SELECT *
FROM Bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

-- Correcting the dates
SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt_test
FROM Bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')


-- Clean data inserted into the Sliver table from the bronze table
SET @start_time = GETDATE();
PRINT'>> Truncating Table: Silver.crm_prd_info';
TRUNCATE TABLE Silver.crm_prd_info;
PRINT'>> Inserting Data Into: crm_prd_info';
INSERT INTO Silver.crm_prd_info(
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
REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other Sales'
WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
FROM Bronze.crm_prd_info
SELECT * FROM Silver.crm_prd_info
SET @end_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)+ 'seconds';
PRINT'>> ------------------';
/*
WHERE REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_') NOT IN
(SELECT DISTINCT id FROM Bronze.erp_px_cat_g1v2)*/
/*
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN(
SELECT sls_prd_key FROM Bronze.crm_sales_details)*/

-- =====================================
-- Sales table
-- =====================================
SET @start_time = GETDATE();
PRINT'>> Truncating Table: Silver.crm_sales_details';
TRUNCATE TABLE Silver.crm_sales_details;
PRINT'>> Inserting Data Into: Silver.crm_sales_details';
INSERT INTO Silver.crm_sales_details(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,

CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
THEN sls_quantity * ABS(sls_price)
ELSE sls_sales
END AS sls_sales,

sls_quantity,

CASE WHEN sls_price IS NULL OR sls_price <= 0
THEN sls_sales/ NULLIF(sls_quantity, 0)
ELSE sls_price
END AS sls_price

FROM Bronze.crm_sales_details
WHERE sls_prd_key NOT IN(SELECT prd_key FROM Silver.crm_prd_info)
SET @end_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)+ 'seconds';
PRINT'>> ------------------';

--Check for Invalid Dates
SELECT
NULLIF(sls_order_dt, 0) sls_order_dt
FROM Bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8

--Check for Invalid Dates Orders
SELECT
*
FROM Bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > Sls_due_dt

--Buisness Rules:
-->>Sales = Quantity * Price
-->>Values must not be null, negative or zeroes.

SELECT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,

CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
THEN sls_quantity * ABS(sls_price)
ELSE sls_sales
END AS sls_sales, --Recaalculate sales if original value is missing or incorrect

CASE WHEN sls_price IS NULL OR sls_price <= 0
THEN sls_sales/ NULLIF(sls_quantity, 0)
ELSE sls_price
END AS sls_price -- Drive price if original value is invalid


FROM Bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales,sls_quantity, sls_price

SELECT * FROM Silver.crm_sales_details


-- =================================
-- Erp_cust_az12 table
-- =================================
SET @start_time = GETDATE();
PRINT'>> Truncating Table: Silver.erp_cust_az12';
TRUNCATE TABLE Silver.erp_cust_az12;
PRINT'>> Inserting Data Into: Silver.erp_cust_az12';

INSERT INTO Silver.erp_cust_az12(cid, bdate, gen)
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
ELSE cid
END cid,

CASE WHEN bdate > GETDATE() THEN NULL
 ELSE bdate
END AS bdate,

CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
     ELSE 'n/a'
END AS gen
FROM Bronze.erp_cust_az12
SET @end_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)+ 'seconds';
PRINT'>> ------------------';

SELECT * FROM Silver.erp_cust_az12
-- Identify Out-of-Range Dates

SELECT DISTINCT
bdate
FROM Bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Data Standardization & Consistency
SELECT DISTINCT gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
     ELSE 'n/a'
END AS gen
FROM Bronze.erp_cust_az12

-- =========================
-- erp_loc_a101 table
-- =========================
SET @start_time = GETDATE();
PRINT'>> Truncating Table: Silver.erp_loc_a101';
TRUNCATE TABLE Silver.erp_loc_a101;
PRINT'>> Inserting Data Into: Silver.erp_loc_a101';
INSERT INTO Silver.erp_loc_a101 
(
cid,
cntry
)
SELECT
REPLACE(cid, '-', '') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
     WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
     ELSE TRIM(cntry)
END AS cntry
FROM Bronze.erp_loc_a101 
SET @end_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)+ 'seconds';
PRINT'>> ------------------';
-- Data Standardization & Consistency
SELECT DISTINCT cntry
FROM Bronze.erp_loc_a101

SELECT * FROM SILVER.erp_loc_a101 

-- ===========================
-- erp_px_cat_g1v2 table
-- ===========================
SET @start_time = GETDATE();
PRINT'>> Truncating Table: Silver.erp_px_cat_g1v2';
TRUNCATE TABLE Silver.erp_px_cat_g1v2;
PRINT'>> Inserting Data Into: Silver.erp_px_cat_g1v2';
INSERT INTO Silver.erp_px_cat_g1v2
(
id,
cat,
subcat,
maintenance
)
SELECT
id,
cat,
subcat,
maintenance
FROM Bronze.erp_px_cat_g1v2
SET @end_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)+ 'seconds';
PRINT'>> ------------------';

-- Check for unwanted spaces
SELECT * FROM Bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data Standardization & Consistency
SELECT DISTINCT
maintenance
FROM Bronze.erp_px_cat_g1v2

SELECT * FROM Silver.erp_px_cat_g1v2
SET @batch_end_time = GETDATE();
    PRINT'=============================='
    PRINT'Loading Silver Layer is completed';
    PRINT' -Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds'
    PRINT'==============================='

END TRY
BEGIN CATCH
    PRINT'===================================='
    PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER'
    PRINT'Error Message' + ERROR_MESSAGE();
    PRINT'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
    PRINT'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
    PRINT'====================================='
END CATCH
END

EXEC Bronze.load_Bronze
EXEC Silver.load_Silver
