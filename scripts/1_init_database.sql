
/*

==========================================================================================================

Create Database and Schemas

Script Purpose:
	This script creates a new database named 'DataWarehouse' after checking if it already exists.
	If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
	within the database: 'bronze', 'silver', and 'gold'.

WARNING:

	Running this script will drop the entire 'DataWarehouse' database if it exists.
	All data in that database will be permanently deleted. Proceed with caution
	and ensure you have proper backups before running this script.

==========================================================================================================

*/

use master ;

Go 

---> Frop and Recreate the Data Warehouse 

If Exists (Select 1 From sys.databases Where name = 'DataWarehouse')

Begin	
	Alter Database DataWarehouse Set single_user With Rollback Immediate ;
	Drop  Database DataWarehouse ;

End ;


---> Create the 'DataWarehouse' database 

Go

Create Database DataWarehouse ;

---> Create needed Schemas :

Go

use DataWarehouse ;

Go
Create schema bronze ;

Go
Create schema silver ;

create schema gold ;
