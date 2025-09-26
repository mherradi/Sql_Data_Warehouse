/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================

Script Purpose:
This stored procedure performs the ETL iatract, Transform, Load) process to
populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
- Truncates Silver tables.
- Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC Silver.load_silver;

===============================================================================

*/


Create or Alter Procedure  silver.load_silver As

Begin 
	Begin Try
		
		Print '==========================================================================================================' ;
		Print 'Clean & Loading Tables Into Silver Layer Tables' ;
		Print '==========================================================================================================' ;

		Declare @start_time Datetime, @end_time Datetime, @batch_start_time Datetime, @batch_end_time  Datetime ;


		Set @batch_start_time = GETDATE() ;

		/* ================================================= Clean and Load crm_cust_info ================================== */

		Print '----------------------------------------------------------------------------------------------------------' ;
		Print 'Clean and Load crm_cust_info Table' ;
		Print '----------------------------------------------------------------------------------------------------------' ;

		Set @start_time = GETDATE();

		print('>> Truncating Table : silver.crm_cust_info');

		Truncate Table silver.crm_cust_info  ;

		Print('>> Inserting Data Into : silver.crm_cust_info');

		INSERT INTO silver.crm_cust_info (
					cst_id, 
					cst_key, 
					cst_firstname, 
					cst_lastname, 
					cst_marital_status, 
					cst_gndr,
					cst_create_date
		)
		SELECT
					cst_id,
					cst_key,
					TRIM(cst_firstname) AS cst_firstname,
					TRIM(cst_lastname) AS cst_lastname,
					CASE 
						WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
						WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
						ELSE 'n/a'
					-- Normalize marital status values to readable format
					END AS cst_marital_status, 
					CASE 
						WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
						WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
						ELSE 'n/a'
					-- Normalize gender values to readable format
					END AS cst_gndr, 
					cst_create_date
		FROM (
					SELECT
						*,
						ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
					FROM bronze.crm_cust_info
					WHERE cst_id IS NOT NULL
		) t

		WHERE flag_last = 1 ;

		Set @end_time = GETDATE() ;

		Print 'Clean & Load Duration : ' + Cast( DateDiff(second , @Start_time , @End_time) as Nvarchar ) + ' seconds.'

		Print '**********************************************************************************************' ;


		/* ======================================================  Clean and Load crm_prd_info ================================== */

	
		Print '----------------------------------------------------------------------------------------------------------' ;
		Print 'Clean and Load crm_prd_info Table' ;
		Print '----------------------------------------------------------------------------------------------------------' ;

		Set @start_time = GETDATE();


		print('>> Truncating Table : silver.crm_prd_info')

		Truncate Table silver.crm_prd_info  ;

		Print('>> Inserting Data Into : silver.crm_prd_info');

		Insert Into silver.crm_prd_info(

			prd_id			 ,
			cat_id			 ,
			prd_key			 ,
			prd_nm			 ,
			prd_cost		 ,
			prd_line		 ,
			prd_start_dt	 ,
			prd_end_dt		 

		)

		select

			prd_id			,
			REPLACE(SUBSTRING(prd_key , 1 , 4)  , '-','_') as cat_id			,
			SUBSTRING(prd_key , 7 , LEN(prd_key)) as prd_key	,
			prd_nm			,
			ISNULL(prd_cost , 0) prd_cost		,
			case  UPPER(TRIM(prd_line))	
				when 'M' then 'Mountain'
				when 'R' then 'Road'
				when 'S' then 'Other Sales'
				when 'T' then 'Touring'
				else 'n/a'
			end as prd_line ,
			CAST(prd_start_dt as Date)	,
			Cast( DATEADD(day, -1, LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt) ) as Date ) as prd_end_dt 

		from
			bronze.crm_prd_info ;


		Set @end_time = GETDATE() ;

		Print 'Clean & Load Duration : ' + Cast( DateDiff(second , @Start_time , @End_time) as Nvarchar ) + ' seconds.'

		Print '**********************************************************************************************************' ;



		/* ================================================= Sales Details Table Before ================================== */

	
	
		Print '----------------------------------------------------------------------------------------------------------' ;
		Print 'Clean and Load crm_sales_details Table' ;
		Print '----------------------------------------------------------------------------------------------------------' ;

		Set @start_time = GETDATE();

		print('>> Truncating Table : silver.crm_sales_details')

		Truncate Table silver.crm_sales_details  ;

		Print('>> Inserting Data Into : silver.crm_sales_details')

		Insert Into silver.crm_sales_details (

			sls_ord_num		 ,
			sls_prd_key		 ,
			sls_cust_id		 ,
			sls_order_dt	 ,
			sls_ship_dt		 ,
			sls_due_dt		 ,
			sls_sales		 ,
			sls_quantity	 ,
			sls_price		 
		)


		select 
			sls_ord_num		,
			sls_prd_key		,
			sls_cust_id		,
			case 
				when sls_order_dt <= 0  or len(sls_order_dt) != 8 Then Null
				else cast(cast(sls_order_dt as varchar) as Date )
			end as sls_order_dt ,

			case 
				when sls_ship_dt <= 0 or len(sls_ship_dt) != 8 Then Null
				else cast(cast(sls_ship_dt as varchar) as Date )
			end as sls_ship_dt ,

			case 
				when sls_due_dt <= 0 or len(sls_due_dt) != 8 Then Null
				else cast(cast(sls_due_dt as varchar) as Date )
			end as sls_due_dt ,

			case 
				when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
					then sls_quantity * sls_price
				else sls_sales
			end as sls_sales ,

			sls_quantity	,

			case 
				when sls_price is null or sls_price <= 0
					then sls_sales /  NULLIF(sls_quantity , 0 )
				else sls_price
			end as sls_price
	
		from
			bronze.crm_sales_details ;

		Set @end_time = GETDATE() ;

		Print 'Clean & Load Duration : ' + Cast( DateDiff(second , @Start_time , @End_time) as Nvarchar ) + ' seconds.'

		Print '**********************************************************************************************************' ;


		/* ================================================= Clean and Load erp_cust_az12 ================================== */

	
		Print '----------------------------------------------------------------------------------------------------------' ;
		Print 'Clean and Load erp_cust_az12  Table' ;
		Print '----------------------------------------------------------------------------------------------------------' ;

		Set @start_time = GETDATE() ;


		print('>> Truncating Table : silver.erp_cust_az12')

		Truncate Table silver.erp_cust_az12  ;

		Print('>> Inserting Data Into : silver.erp_cust_az12')

		insert into silver.erp_cust_az12 (

			cid		 ,
			bdate	 ,
			gen		 

		)


		select
			case 
				when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
				else cid
			end as cid		 ,
			case
				when bdate > GETDATE() then null
				else bdate
			end as bdate	 ,
			case
				when UPPER(TRIM(gen)) in ('F','FEMALE')  then 'Female'
				when UPPER(TRIM(gen)) in ('M','MALE')  then 'Male'
				else 'n/a'
			end as gen		 			

		from
			bronze.erp_cust_az12 ;


		Set @end_time = GETDATE() ;

		Print 'Clean & Load Duration : ' + Cast( DateDiff(second , @Start_time , @End_time) as Nvarchar ) + ' seconds.';

		Print '**********************************************************************************************************' ;

		/* =================================================   Clean and Load erp_loc_a101  ================================== */

	
		Print '----------------------------------------------------------------------------------------------------------' ;
		Print 'Clean and Load erp_loc_a101  Table' ;
		Print '----------------------------------------------------------------------------------------------------------' ;

		Set @start_time = GETDATE() ;

		print('>> Truncating Table : silver.erp_loc_a101');

		Truncate Table silver.erp_loc_a101  ;

		Print('>> Inserting Data Into : silver.erp_loc_a101');

		insert into silver.erp_loc_a101 (

			cid ,
			cntry

		)


		select
			REPLACE(cid,'-','')	 cid	,
	
			case	
				when trim(cntry) = 'DE' then 'Germany' 
				when trim(cntry) in ('US','USA', 'United States') then 'United States'
				when trim(cntry) is null or trim(cntry) = '' then 'n/a'
				else trim(cntry)
			end as cntry

		from
			bronze.erp_loc_a101 ;


		Set @end_time = GETDATE() ;

		Print 'Clean & Load Duration : ' + Cast( DateDiff(second , @Start_time , @End_time) as Nvarchar ) + ' seconds.';

		Print '**********************************************************************************************************' ;

		/* =================================================   Clean and Load Erp_px_cat_g1v2  ================================== */

		Print '----------------------------------------------------------------------------------------------------------' ;
		Print 'Clean and Load erp_px_cat_g1v2  Table' ;
		Print '----------------------------------------------------------------------------------------------------------' ;

		Set @start_time = GETDATE() ;


		print('>> Truncating Table : silver.erp_px_cat_g1v2');

		Truncate Table silver.erp_px_cat_g1v2  ;

		Print('>> Inserting Data Into : silver.erp_px_cat_g1v2');

		Insert Into silver.erp_px_cat_g1v2(

			id				,
			cat				,
			subcat			,
			maintenance		
		)

		select
	
			id				,
			cat				,
			subcat			,
			maintenance	
	
		from
			bronze.erp_px_cat_g1v2 ;


		Set @end_time = GETDATE() ;

		Print 'Clean & Load Duration : ' + Cast( DateDiff(second , @Start_time , @End_time) as Nvarchar ) + ' seconds.'

		Print '**********************************************************************************************************' ;


		Set @batch_end_time = GETDATE() ;


		Print '----------------------------------------------------------------------------------------------------------' ;

		Print 'Batch Cleaning & Loading Duration : ' + Cast( DateDiff(second , @batch_start_time , @batch_end_time) as Nvarchar ) + ' seconds.' ;
		
		Print '----------------------------------------------------------------------------------------------------------' ;
	
	End Try

	Begin Catch
		Print '==============================================================================================' ;
		Print 'An Error Occured During Loading Bronze Layer:' ;
		Print 'Error Message : ' + Error_Message() ;
		Print 'Error Number   :' + Cast(Error_Number() as Nvarchar(50));
		Print '==============================================================================================' ;
	End	Catch


End


--------------------------------------------------------------- >> Procedure in Charge << -------------------------------------------------

Exec silver.load_silver ;

--------------------------------------------------------------------------------------------------------------------------------------------
