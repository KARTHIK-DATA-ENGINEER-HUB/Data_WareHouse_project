/*
==================================================
Stored Procedure:Load silver layer(bronze-> silver)
===================================================
Script Purpose:
  This stored procedure loads data into the silver schema from the bronze schema.
  It performs the following actions:
    -Truncates the silver tables before loading data.
    -uses the  INSERT commmand to load transformed and cleansed data from bronze to silver schema.
Parameters:
  This Stored procedure does not accept any parameters or return any values.
Usage Example:
  EXEC silver.load_silver;
====================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
	---------------------------------------------------
	TRUNCATE TABLE silver.crm_cust_info;
	INSERT INTO silver.crm_cust_info(
	cst_idd,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)
	SELECT 
		cst_idd,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,--removing the leading and trailing white spaces
		TRIM(cst_lastname) AS cst_lastname,--removing the leading and trailing white spaces
		CASE WHEN TRIM(UPPER(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN TRIM(UPPER(cst_gndr))='M' THEN 'Married'
			 ELSE 'n/a'
		END AS cst_marital_status,------making the data standardization-------
	 
		CASE WHEN TRIM(UPPER(cst_gndr)) = 'F' THEN 'Female'
			 WHEN TRIM(UPPER(cst_gndr))='M' THEN 'Male'
			 ELSE 'n/a'
		END AS cst_gender,------making the data standardization-------
		cst_create_date
	FROM (
	SELECT 
		*,
		ROW_NUMBER() over(partition by cst_idd order by cst_create_date desc) as flag_latest 
	FROM bronze.crm_cust_info WHERE cst_idd IS NOT NULL) t
	WHERE flag_latest=1  --removing the duplicates in the pk col by preserving the latest record 
	----------------------------------------------------------------
	TRUNCATE TABLE SILVER.crm_prd_info
	INSERT INTO SILVER.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt)
	SELECT
 		prd_id,
		REPLACE(SUBSTRING(PRD_KEY,1,5),'-','_') AS cat_id,--REMOVING LEADING AND TRAILING WHITE SPACES
		SUBSTRING(PRD_KEY,7,LEN(PRD_KEY)) AS prd_key,--REMOVING LEADING AND TRAILING WHITE SPACES
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost, 
		CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
			 WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'--DATA CONSISTENCY AND STANDARDIZATION
			 WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
			 WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
			 ELSE 'n/a'
		END AS
		prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		--FIXING THE END DATE COLUMN WHICH HAS END DATE <START DATE
		CAST(LEAD(prd_start_dt) over(partition by  prd_key order by prd_start_dt)-1 AS DATE) as prd_end_dt
	FROM BRONZE.CRM_PRD_INFO
	-----------------------------------------------------------------------
	TRUNCATE TABLE silver.crm_sales_details;
	INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt ,
	sls_sales ,
	sls_quantity ,
	sls_price 
	 )
	SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		---CLEANING THE DATE COLUMNS WITH DATES=0,LEN(DATE)!=8 WITH NULL AND CONVERTING THE DATA TYPE FROM INT TO VARCHAR AND THEN TO DATE
		CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt) != 8 THEN NULL 
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS
		sls_order_dt,
		CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt) != 8 THEN NULL 
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS
		sls_ship_dt,
		CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt) != 8 THEN NULL 
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS
		sls_due_dt,
		CASE WHEN  sls_sales IS NULL 
		OR sls_sales<=0 OR sls_sales != sls_quantity*ABS(sls_price) 
		THEN sls_quantity*ABS(sls_price)
		ELSE sls_sales 
		END AS
		sls_sales,--recalculate sales it has missing or incorrect values
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price<=0 THEN
		sls_sales/NULLIF(sls_quantity,0)
		ELSE sls_price
		END AS
		sls_price--derives price if the original price is invalid 
	FROM bronze.crm_sales_details

	--IF SALES IS NEGATIVE OR 0 OR NULL THEN USE PRICE AND QUANTITY TO DERIVE THE SALES
	--IF PRICE ARE NEGATIVE OR NULL OR ZERO THEN DERIVE USING SALES
	--IF PRICE IS NEGATIVE CONVERYT IT TO POSITIVE
	-------------------------------------------------------------
	TRUNCATE TABLE silver.erp_cust_az12;
	INSERT INTO silver.erp_cust_az12(
	cid,bdate,gen)
	SELECT
		CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
		ELSE  CID
		END AS CID,--remove 'nas' prefix if present
		CASE WHEN BDATE > GETDATE() THEN NULL
		ELSE BDATE
		END AS BDATE,--set future bdates to null
		CASE WHEN UPPER(TRIM(GEN)) IN('F','FEMALE') THEN  'Female'
			 WHEN UPPER(TRIM(GEN)) IN('M','MALE') THEN  'Male'
		ELSE 'n/a'
		END AS GEN--normalize gender values and handle unknown cases
	FROM bronze.erp_cust_az12
	------------------------------------------------------------------
	TRUNCATE TABLE silver.erp_loc_a101
	INSERT INTO silver.erp_loc_a101(CID,CNTRY)
	SELECT 
		REPLACE(CID,'-','') AS CID,--REPLACE '-' WITH '' SO THAT KEY COULD BE DERIVED TO JOIN BOTH THE TABLES
		CASE WHEN TRIM(UPPER(CNTRY)) IN ('US','UNITED STATES','USA') THEN 'United States' 
			WHEN TRIM(UPPER(CNTRY)) IN ('DE','GERMANY') THEN 'Germany'
			WHEN TRIM(CNTRY) ='' OR TRIM(CNTRY) IS NULL THEN 'n/a'
			ELSE TRIM(CNTRY)
		END AS CNTR--NORMALIZE AND HANDLE MISSING OR BLANK COUTRY CODE
	FROM bronze.erp_loc_a101
	-----------------------------------------------------------------
	TRUNCATE TABLE SILVER.erp_px_cat_g1v2
	INSERT INTO SILVER.erp_px_cat_g1v2(ID,
		CAT,
		SUBCAT,
		MAINTENANCE)
	SELECT 
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
	FROM bronze.erp_px_cat_g1v2
	END TRY
	BEGIN CATCH
		PRINT'ERROR OCCURED WHILE LOADING SILVER LAYER'
		PRINT'Error Messaege'+ERROR_MESSAGE();
		PRINT'Error Messaege'+CAST(ERROR_NUMBER() AS NVARCHAR);
	END CATCH
END
