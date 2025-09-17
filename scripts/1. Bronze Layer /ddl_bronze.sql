
/*

============================================================================================
DDL Script: Create Bronze Tables
============================================================================================
Script Purpose:
This script creates tables in the 'bronze' schema, dropping existing tables
if they already exist.
Run this script to re-define the DDL structure of 'bronze' Tables

============================================================================================

*/


---> Creating Customers Info Table

If Object_Id('bronze.crm_cust_info' , 'U') IS NOT NULL
	Drop Table bronze.crm_cust_info ;

Go

Create Table bronze.crm_cust_info (

	cst_id			      	Int ,
	cst_key			      	Nvarchar(50),
	cst_firstname		    Nvarchar(50),
	cst_lastname		    Nvarchar(50),
	cst_marital_status	Nvarchar(50),
	cst_gndr			      Nvarchar(50),
	cst_create_date		  Date


) ;

---> Creating Products Info Table

If Object_Id('bronze.crm_prd_info' , 'U') IS NOT NULL
	Drop Table bronze.crm_prd_info ;

Go 
Create Table bronze.crm_prd_info(

	prd_id			  Int,
	prd_key			  Nvarchar(50),
	prd_nm			  Nvarchar(50),
	prd_cost		  Int,
	prd_line	  	Nvarchar(50),
	prd_start_dt	Date,
	prd_end_dt		Date

) ;

---> Creating Sales Details Table

If Object_Id('bronze.crm_sales_details' , 'U') IS NOT NULL
	Drop Table bronze.crm_sales_details ;

Go 

Create Table bronze.crm_sales_details (

	sls_ord_num		Nvarchar(50),
	sls_prd_key		Nvarchar(50),
	sls_cust_id		Int ,
	sls_order_dt	Int ,
	sls_ship_dt		Int ,
	sls_due_dt		Int ,
	sls_sales		  Int ,
	sls_quantity	Int ,
	sls_price	  	Int 


) ;



---> Creating Customer Table

If Object_Id('bronze.erp_cust_az12' , 'U') IS NOT NULL
	Drop Table bronze.erp_cust_az12 ;

Go 

Create Table bronze.erp_cust_az12 (

	cid		Nvarchar(50),
	bdate	Date ,
	gen		Nvarchar(50)

);


---> Creating Location Table

If Object_Id('bronze.erp_loc_a101' , 'U') IS NOT NULL
	Drop Table bronze.erp_loc_a101 ;

Go

Create Table bronze.erp_loc_a101(

	cid		Nvarchar(50),
	cntry	Nvarchar(50)
) ;


---> Creating Categories Table

If Object_Id('bronze.erp_px_cat_g1v2' , 'U') IS NOT NULL
	Drop Table bronze.erp_px_cat_g1v2 ;

Go

Create Table bronze.erp_px_cat_g1v2(

	id				    Nvarchar(50),
	cat			    	Nvarchar(50),
	subcat			  Nvarchar(50),
	maintenance		Nvarchar(50),

);
