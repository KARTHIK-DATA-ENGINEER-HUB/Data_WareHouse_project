-----------------------------------CREATE DATABASE AND SCHEMAS------------------------------
/* This script creats a database by checking if it exists or not if database exists it will drop the database and create a 
new one.It also creates the schemas bronze,silver and gold with in this database Datawarehousee*/

/***WARNING:This script will delete all the data inside the database.please ensure that you have a backup of the data in the database before 
using the script*/
----------------------------------CREATE DATABASE Datawarehouse------------------------------
USE MASTER;
IF EXISTS(SELECT 1 FROM sys.databases  WHERE NAME='Datawarehouse')
BEGIN
	DROP DATABSE Datawarehouse
END
GO
CREATE DATABASE Datawarehouse;
USE Datawarehouse
GO
-------------------------------------CREATING SCHEMAS-----------------------------------------
CREATE SCHEMA bronze
GO
CREATE SCHEMA silver
GO
CREATE SCHEMA gold
