/*
-------------------------------------------------------------------
CREATE DATABASE & SCHEMAS
-------------------------------------------------------------------
# This code checks whether the a database called 'DataWarehouse' already exists. 
- If it does exist, this database will be dropped. After the existing database is dropped, a new database 'DataWarehouse' is created, along with the schemas, 'bronze', 'silver', and 'gold'.
- If a database 'DataWarehouse' does not exist, no tables are dropped, and a new table with the same name is created.

# Three schemas, 'bronze', 'silver', and 'gold' are also created along with the new database.

*/
-- Change to 'master' database

-- GO separates batches when working with multiple SQL statements
-- Tells SQL to completely finish executing a command before moving to the next command

USE master;
GO

-- Check if the 'DataWarehouse' database exists, and drop it if exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create a database 'DataWarehouse'

CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- Creating the schemas

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
