/*
========================================================
quality checks
=========================================================
scripts purpose:
  This script performs various quality checks for the data consistency ,accuracy
and standardizaiton across the silver layer.It includes checks for:
  -Null or duplicate primary keys.
  -unwanted spaces in string fields.
  -data standardization and consistency.
  -invalid date range and orders
  -data consistency between the related fields
Usage Notes:
  -Run these checks after data loading to silver layer.
  -investigate and resolve any discreapancies found during the checks


---------------------QUALITY CHECK in the bronze layer
---CHECK FOR DUPLICATES IN THE PRIMARY KEY COLUMN OR IF ANY NULLS EXISTS IN PK COL 
SELECT cst_idd,count(*) FROM bronze.crm_cust_info
group by cst_idd
having count(*)>1 OR cst_idd IS NULL
------CHECKING THE UNWANTED SPACES IN THE STRIN GCOLUMNS
SELECT cst_firstname FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT cst_gndr FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)
----data standardizationa and consistency
select distinct(cst_gndr) from bronze.crm_cust_info

select distinct(cst_marital_status) from bronze.crm_cust_info

-----checking the transformation code written on top of bronze ie silver layer whether transformations worked or not
---CHECK FOR DUPLICATES IN THE PRIMARY KEY COLUMN OR IF ANY NULLS EXISTS IN PK COL 
SELECT cst_idd,count(*) FROM silver.crm_cust_info
group by cst_idd
having count(*)>1 OR cst_idd IS NULL


SELECT cst_firstname FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

select distinct(cst_gndr) from silver.crm_cust_info

select distinct(cst_marital_status) from silver.crm_cust_info



--QUALITY CHECK BRONZE.CRM_PRD_INFO
SELECT PRD_ID,COUNT(*) FROM BRONZE.CRM_PRD_INFO 
GROUP BY PRD_ID
HAVING COUNT(*)>1--NO DUPLICATES IN THE PK COL OF BRONZE.CRM_PRD_INFO

SELECT prd_nm FROM bronze.CRM_PRD_INFO
WHERE prd_nm != TRIM(prd_nm)--CHECK FOR ANY TRAIL OR LEAD SPACES IN BRONZE

SELECT * FROM bronze.CRM_PRD_INFO
WHERE prd_cost IS NULL OR prd_cost<0 --CHECK FOR ANY NEGATIVE OR NULL VALUES IN COST COL IN BRONZE LAYER FOR PRD_INFO

SELECT DISTINCT(prd_line) FROM bronze.CRM_PRD_INFO--FOR DATA CONSISTENCY AND STANDARDIZATION




--DATAQUALITY CHECK FOR SILVER_CRM_PRD_INFO AFTER LOADING DATA FROM BRONZE
SELECT PRD_ID,COUNT(*) FROM SILVER.CRM_PRD_INFO 
GROUP BY PRD_ID
HAVING COUNT(*)>1--CKECK FOR DUPLICATES IN PK COL IN SILVER_CRM_PRD_INFO

SELECT prd_nm FROM silver.CRM_PRD_INFO
WHERE prd_nm != TRIM(prd_nm)--CHECK FOR TRAIL AND LEAD SPACS IN SILVER_CRM_PRD_INFO

SELECT * FROM silver.CRM_PRD_INFO
WHERE prd_cost IS NULL OR prd_cost<0--CHECK FOR COST IF NEGATIVE OR NULL

SELECT * FROM silver.CRM_PRD_INFO
WHERE prd_start_dt>prd_end_dt---CHECK FOR IF START DATE IS GRETAER THAN END DATE
 
----DATA QUALITY CHECK FOR BRONZE_CRM_SALES_DETAILS
SELECT sls_ord_num FROM bronze.crm_sales_details
WHERE sls_ord_num != trim(sls_ord_num)--CHECKS WHITE SPACES

SELECT sls_prd_key FROM bronze.crm_sales_details
WHERE sls_prd_key != trim(sls_prd_key)--CHECKS WHITE SPACES

--CHECKIING THE COMPATABILITY TO JOIN THE TABLES
select * from bronze.crm_sales_details
WHERE sls_prd_key not in (select prd_key from bronze.crm_prd_info)-

select * from bronze.crm_sales_details
WHERE sls_prd_key not in (select cst_idd from bronze.crm_cust_info)

SELECT NULLIF(sls_ship_dt,0) FROM bronze.crm_sales_details
where sls_ship_dt<=0 OR LEN(sls_ship_dt) != 8 OR sls_ship_dt>20260303
OR sls_ship_dt<19000101

SELECT sls_order_dt FROM bronze.crm_sales_details
where LEN(sls_order_dt) != 8

SELECT sls_ship_dt FROM bronze.crm_sales_details
where sls_ship_dt<0

SELECT sls_due_dt FROM bronze.crm_sales_details
where sls_due_dt<0

SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt>sls_ship_dt OR sls_order_dt>sls_due_dt--NO RECORDS FOUND
--CHECK FOR DATA INCONSISTENCY IN THE SALES_DETAILS
SELECT DISTINCT 
sls_sales AS OLD_SLS_SALES,
CASE WHEN  sls_sales IS NULL 
	OR sls_sales<=0 OR sls_sales != sls_quantity*ABS(sls_price) 
	THEN sls_quantity*ABS(sls_price)
	ELSE sls_sales 
	END AS
	sls_sales,
sls_quantity,
sls_price AS OLD_SLS_PRICE,
CASE WHEN sls_price IS NULL OR sls_price<=0 THEN
	sls_sales/NULLIF(sls_quantity,0)
	ELSE sls_price
	END AS
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales<=0 OR sls_quantity<=0 OR sls_price<=0
ORDER BY sls_sales,
sls_price 

--checking if the silver layer has any inconsistent data in the sales,price,quantity columns
select sls_ord_num from silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales<=0 OR sls_quantity<=0 OR sls_price<=0
ORDER BY sls_sales,
sls_price ---no records found
--checking records if the order date is higher than the ship and the due date
select sls_ord_num from silver.crm_sales_details
where sls_order_dt>sls_ship_dt or sls_order_dt>sls_due_dt
---------BRONZE_ERP_CUST_INFO
--CHECK IF ANY BDATE IS OLDER THAN THE REQUIRED BUSINEES DATE
SELECT BDATE FROM bronze.erp_cust_az12
WHERE BDATE<'1924-01-01' OR BDATE>GETDATE()--FOUND IRRELEVENT RECORDS
--IF ANY INCONSISTENT DATA IN THE GENDER COL OF BRONZE_ERP_CUST_INFO
SELECT DISTINCT(GEN) FROM bronze.erp_cust_az12
--checking the silver erp-cust table 
SELECT DISTINCT(GEN) FROM silver.erp_cust_az12

--checking if bday is greater than the curret date 
SELECT BDATE FROM silver.erp_cust_az12
WHERE BDATE>GETDATE()
---------BRONZE_ERP_LOC_INFO
SELECT 
	DISTINCT(CNTRY),--STANDARDIZING THE COUTRY NAMES
	CASE WHEN TRIM(UPPER(CNTRY)) IN ('US','UNITED STATES','USA') THEN 'United States' 
		WHEN TRIM(UPPER(CNTRY)) IN ('DE','GERMANY') THEN 'Germany'
		WHEN TRIM(CNTRY) ='' OR TRIM(CNTRY) IS NULL THEN 'n/a'
		ELSE TRIM(CNTRY)
	END AS CNTRY
FROM bronze.erp_loc_a101

------CHECKING THE SILVER LAYER AFTER LOADING ERP_LOC_INFO
SELECT 
	DISTINCT(CNTRY) FROM 
silver.erp_loc_a101---WORKING AS EXPECTED-DATA STANDARDIZATION AND CONSISTENCY
----
SELECT  CID FROM silver.erp_loc_a101
WHERE CID LIKE '%-%'--WORKING AS EXPECTED
------CHECHKING THE DATA QUALITY ISSUE IN THE BRONZE.ERP_PX_CAT
---CHECK FOR UNWANTED SPACES
SELECT CAT FROM bronze.erp_px_cat_g1v2
WHERE CAT != TRIM(CAT)--NO RECORDS FOUND
---
SELECT SUBCAT FROM bronze.erp_px_cat_g1v2
WHERE SUBCAT != TRIM(SUBCAT)--NO RECORDS FOUND
--
SELECT MAINTENANCE FROM bronze.erp_px_cat_g1v2
WHERE MAINTENANCE != TRIM(MAINTENANCE)--NO RECORDS FOUND

SELECT DISTINCT(CAT) FROM bronze.erp_px_cat_g1v2--NO ISSUES WITH THIS COL
SELECT DISTINCT(SUBCAT) FROM bronze.erp_px_cat_g1v2--NO ISSUES
SELECT DISTINCT(MAINTENANCE) FROM bronze.erp_px_cat_g1v2--NO ISSUES
---QUALITY CHEKS IN SILVER
--AS WE HAVE NOT MODIFIED ANY THING IN THE BRONZE FOR THIS TABLE NO NEED TO CHECK IN THE SILVER LAYER

