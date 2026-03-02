/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-----creating dimension table custoomer info as a view in the gold layer
if object_id ('gold.dim_customers','V') is not null
	drop view  gold.dim_customers;
	go
CREATE VIEW gold.dim_customers AS(
SELECT 
	ROW_NUMBER() OVER(ORDER BY ci.cst_idd) AS customer_key,
	ci.cst_idd as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	lo.cntry as country,
	ci.cst_marital_status as marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' then ci.cst_gndr
	else coalesce(ca.gen,'n/a')
	END AS gender,
	ca.bdate as birth_date,
	ci.cst_create_date as create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
on  ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101  lo
on ci.cst_key=lo.cid)

-----creating dimension table product info as a view in the gold layer
if object_id ('gold.dim_products','V') is not null
	drop view  gold.dim_products;
	go
create view gold.dim_products as(
select
	ROW_NUMBER() over(order by  pn.prd_start_dt,pn.prd_key) as product_key,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.MAINTENANCE,
	pn.prd_cost as cost,
	pn.prd_line as product_liine,
	pn.prd_start_dt as start_date
from silver.crm_prd_info pn 
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id=pc.id
where pn.prd_end_dt is null--to select on the currrent information
)

-------------creating the  fact table by joining the surrogate  key from dimension tables
if object_id ('gold.fact_sales','V') is not null
	drop view  gold.fact_sales;
	go
create view gold.fact_sales as(
 select 
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price
 from silver.crm_sales_details sd
 left join gold.dim_products pr 
 on sd.sls_prd_key=pr.product_number
 left join gold.dim_customers cu
 on sd.sls_cust_id=cu.customer_id)
 ----checking the foreign key integration 
 select * from gold.fact_sales s
 left join gold.dim_products p
 on s.product_key = p.product_key
 where p.product_key is null


  select * from gold.fact_sales s
 left join gold.dim_customers c
 on s.customer_key = c.customer_key
 where c.customer_key is null
