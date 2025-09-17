/*
===============================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze Layer)
===============================================================================================
Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:
- Truncates the bronze tables before loading data.
- Uses the `BULK INSERT' command to load data from csv Files to bronze tables.

Parameters:

None.

This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC bronze.load_bronze;

*/



Create Or Alter Procedure bronze.load_bronze As

Begin

	Declare @Start_time Datetime , @End_time Datetime , @batch_start_time Datetime , @batch_end_time Datetime ;

	Begin Try

		Set  @batch_start_time = GETDATE() ;
		Print '==============================================================================================' ;
		Print 'Loading Bronze Layer Tables' ;
		Print '==============================================================================================' ;


		Print '----------------------------------------------------------------------------------------------' ;
		Print 'Loading From CRM Sources' ;
		Print '----------------------------------------------------------------------------------------------' ;
	
		--- > Empty CRM Customer table before start Loading

		Set @Start_time = GETDATE() ;
 
		Print '>> Truncating Customers Table From CRM Source : bronze.crm_cust_info ';

		Truncate Table  bronze.crm_cust_info ;

		---> Loading the whole data into the Table

		Print '>> Inserting Data  Into : bronze.crm_cust_info ';

		Bulk Insert  bronze.crm_cust_info

		From	'C:\Users\elect\OneDrive\Bureau\SQL Workspace\SQL Data Warehouse Project\datasets\source_crm\cust_info.csv'
		
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		Set @End_time = GETDATE() ; 

		Print 'Loading Duration : ' + Cast( DateDiff(second , @Start_time , @End_time) as Nvarchar ) + ' seconds.'

		Print '**********************************************************************************************' ;



		--- > Empty CRM Products table before start Loading

		Set @Start_time = GETDATE() ;

		Print '>> Truncating Products Table From CRM Source : bronze.crm_prd_info ';

		Truncate Table bronze.crm_prd_info ;

		---> Loading the whole data into the Table

		Print '>> Inserting Data  Into : bronze.crm_prd_info ';

		Bulk Insert bronze.crm_prd_info 

		From 'C:\Users\elect\OneDrive\Bureau\SQL Workspace\SQL Data Warehouse Project\datasets\source_crm\prd_info.csv'

		With (

			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',' ,
			TABLOCK

		);

		Set @End_time = GETDATE() ; 

		Print 'Loading Duration : ' + Cast( DateDiff(second , @Start_time , @End_time) as Nvarchar ) + ' seconds.' ;

		Print '**********************************************************************************************' ;

		--- > Empty Sales Details table before start Loading

		Set @Start_time = GETDATE() ;

		Print '>> Truncating Sales Details Table From CRM Source : bronze.crm_sales_details ';

		Truncate Table bronze.crm_sales_details ;

		---> Loading the whole data into the Table

		Print '>> Inserting Data  Into : bronze.crm_prd_info ';

		Print '>> Inserting Data  Into : bronze.crm_sales_details ';

		Bulk Insert bronze.crm_sales_details

		From 'C:\Users\elect\OneDrive\Bureau\SQL Workspace\SQL Data Warehouse Project\datasets\source_crm\sales_details.csv'

		With(

			FIRSTROW =2 ,
			FIELDTERMINATOR = ',' ,
			TABLOCK

		);

		Set @End_time = GETDATE() ; 

		Print 'Loading Duration : ' + Cast( DateDiff(second , @Start_time , @End_time) as Nvarchar ) + ' seconds.' ;

		



		Print '----------------------------------------------------------------------------------------------' ;
		Print 'Loading From ERP Sources' ;
		Print '----------------------------------------------------------------------------------------------' ;

		--- > Empty Custmers table before start Loading

		Set @Start_time = GETDATE() ;

		Print '>> Truncating Custmers Table From ERP Source : bronze.erp_cust_az12 ';

		Truncate Table bronze.erp_cust_az12 ;

		---> Loading the whole data into the Table

		Print '>> Inserting Data  Into : bronze.erp_cust_az12 ';

		Bulk Insert bronze.erp_cust_az12 

		From 'C:\Users\elect\OneDrive\Bureau\SQL Workspace\SQL Data Warehouse Project\datasets\source_erp\CUST_AZ12.csv'

		With(

			FIRSTROW =2 ,
			FIELDTERMINATOR = ',' ,
			TABLOCK
		);

		Set @End_time = GETDATE() ; 

		Print 'Loading Duration : ' + Cast( DateDiff(second , @Start_time , @End_time) as Nvarchar ) + ' seconds.' ;

		Print '**********************************************************************************************' ;



		--- > Empty Local table before start Loading

		Set @Start_time = GETDATE() ;

		Print '>> Truncating Local Table From ERP Source : bronze.erp_loc_a101 ';

		Truncate Table bronze.erp_loc_a101 ;

		---> Loading the whole data into the Table

		Print '>> Inserting Data  Into : bronze.erp_loc_a101 ';

		Bulk Insert bronze.erp_loc_a101

		From 'C:\Users\elect\OneDrive\Bureau\SQL Workspace\SQL Data Warehouse Project\datasets\source_erp\LOC_A101.csv'

		With(

			FIRSTROW =2 ,
			FIELDTERMINATOR = ',' ,
			TABLOCK
		);

		Set @End_time = GETDATE() ; 

		Print 'Loading Duration : ' + Cast( DateDiff(second , @Start_time , @End_time) as Nvarchar ) + ' seconds.' ;

		Print '**********************************************************************************************' ;



		--- > Empty Categories table before start Loading

		Set @Start_time = GETDATE() ;

		Print '>> Truncating Categories Table From ERP Source : bronze.erp_px_cat_g1v2 ';

		Truncate Table bronze.erp_px_cat_g1v2 ;

		---> Loading the whole data into the Table

		Print '>> Inserting Data  Into : bronze.erp_px_cat_g1v2 ';

		Bulk Insert bronze.erp_px_cat_g1v2

		From  'C:\Users\elect\OneDrive\Bureau\SQL Workspace\SQL Data Warehouse Project\datasets\source_erp\PX_CAT_G1V2.csv'

		With(

			FIRSTROW =2 ,
			FIELDTERMINATOR = ',' ,
			TABLOCK
		);

		Set @End_time = GETDATE() ; 

		Print 'Loading Duration : ' + Cast( DateDiff(second , @Start_time , @End_time) as Nvarchar ) + ' seconds.' ;


		Set @batch_end_time = GETDATE() ;

		Print '==============================================================================================' ;

		Print 'Batch Loading Duration : ' + Cast( DateDiff(second , @batch_start_time , @batch_end_time) as Nvarchar ) + ' seconds.' ;
		
		Print '==============================================================================================' ;
	
	End Try

	Begin Catch
		Print '==============================================================================================' ;
		Print 'An Error Occured During Loading Bronze Layer:' ;
		Print 'Error Message : ' + Error_Message() ;
		Print 'Error Number   :' + Cast(Error_Number() as Nvarchar(50));
		Print '==============================================================================================' ;
	End	Catch


End




