/* 
============================================================
    📦 Data Warehouse Setup Script
============================================================
# Script Purpose:
    This script creates a Data Warehouse environment 
    with layered schemas (Bronze, Silver, Gold) used 
    for structured data processing.

    - Uses the master database
    - Creates a new database: DataWarehouse
    - Adds three schemas for ELT/ETL architecture
        • bronze  → raw data
        • silver  → cleaned & transformed data
        • gold    → final analytics-ready data

    Author: Yuvraj Yadav
    Repository: Data Warehouse & Analytics Project
============================================================
*/


-- Going in the Master Dataabase
use master;
GO

-- Create a database name like a DataWarehouse
create Database DataWarehouse;
GO

-- Going to the Database we should be Creating
use DataWarehouse;
GO

-- Create a Three Schemas for the Bronze, Silver, Gold
Create Schema bronze;
GO

Create Schema silver;
GO

Create Schema gold;
GO
