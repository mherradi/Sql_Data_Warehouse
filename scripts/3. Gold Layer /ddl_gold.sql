/*
===================================================================================================================================
DDL Script: Create Gold Views
===================================================================================================================================
Script Purpose:
This script creates views for the Gold layer in the data warehouse.
The Gold layer represents the final dimension and fact tables (Star Schema)

Each view performs transformations and combines data from the Silver layer
to produce a clean, enriched, and business-ready dataset.

Usage:
- These views can be queried directly for analytics and reporting.
===================================================================================================================================
*/



/*-------------------------------------------- Create Dimension Customers Table (View) --------------------------------------------*/

IF OBJECT_ID('gold.dim_customers','V') IS NOT NULL
	
	DROP VIEW gold.dim_customers ;

GO

Create View gold.dim_customers As 

select
	
	-- Surrogate Key 
	ROW_NUMBER() OVER (Order By ci.cst_id )  As customer_key,

	-- Columns From Customers Info Table
	ci.cst_id				As customer_id ,
	ci.cst_key				As customer_number ,
	ci.cst_firstname		As first_name,
	ci.cst_lastname			As last_name ,

	-- Column From ERP Customers Location 
	loc.cntry				As country ,

	ci.cst_marital_status	As marital_status ,
	

	-- Column Integration : Getting the best Data from Commun Columns

	case
		when ci.cst_gndr != 'n/a' then ci.cst_gndr
		else COALESCE(ca.gen ,'n/a') 
	end as gender  ,

	-- Columns From ERP Customers AZ12
	ca.bdate				As birth_date   ,

	ci.cst_create_date		As create_date


From	
	silver.crm_cust_info  ci

	Left Join silver.erp_cust_az12 ca

	ON	ci.cst_key = ca.cid

	Left Join silver.erp_loc_a101 loc

	ON	ci.cst_key = loc.cid ;




/*-------------------------------------------- Create Dimension Products Table (View) --------------------------------------------*/


IF OBJECT_ID('gold.dim_products','V') IS NOT NULL
	
	DROP VIEW gold.dim_products ;

GO

Create View gold.dim_products As

select 

	-- Surrogate Key 
	ROW_NUMBER() OVER (Order By prd.prd_start_dt , prd.prd_key	)  As product_key ,

	prd.prd_id			As product_id ,
	prd.prd_key			As product_number  ,
	prd.prd_nm			As product_name ,

	prd.cat_id			As category_id,
	pc.cat				AS category ,
	pc.subcat			As subcategory ,
	pc.maintenance		,

	prd.prd_cost		As cost,
	prd.prd_line		As product_line,
	prd.prd_start_dt	As start_date 
	
	
	

from
	silver.crm_prd_info prd 

	Left Join silver.erp_px_cat_g1v2 pc

	ON   prd.cat_id  = pc.id


-- Filter Out All Historical Data
Where
	prd.prd_end_dt IS NULL ;



/*-------------------------------------------- Create the Fact Sales Table (View) --------------------------------------------*/


IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
	
	DROP VIEW gold.fact_sales ;

GO

Create View gold.fact_sales as


select

	sd.sls_ord_num		as order_number,
	
	--Using the Generated Surrogate Key to connect the Data Model

	pr.product_key		as product_key,
	
	cu.customer_key		as customer_key,

	sd.sls_order_dt		as order_date	,
	sd.sls_ship_dt		as ship_date		,
	sd.sls_due_dt		as due_date		,
	sd.sls_sales		as sales_date	,
	sd.sls_quantity		as quantity		,
	sd.sls_price		as price
	
from
	silver.crm_sales_details sd

	Left Join gold.dim_products pr

	ON pr.product_number = sd.sls_prd_key 

	Left Join gold.dim_customers cu

	ON  cu.customer_id = sd.sls_cust_id ;



	
/*-------------------------------------------- Connecting the Whole Data Model  --------------------------------------------*/



select * 

from
	gold.fact_sales f

	Left Join gold.dim_customers cu

	On f.customer_key = cu.customer_key 

	Left Join gold.dim_products pr

	On f.product_key = pr.product_key

where

	-- Data Integration Checks
	cu.customer_key IS NULL Or pr.product_key IS NULL
