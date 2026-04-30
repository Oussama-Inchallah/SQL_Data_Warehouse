use master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE
        name = 'DataWarehouse'
) BEGIN
ALTER DATABASE DataWarehouse
SET
    SINGLE_USER
WITH
ROLLBACK IMMEDIATE;

DROP DATABASE DataWarehouse;

END;
GO

create database DataWarehouse;
GO

use DataWarehouse;
GO

create schema bronze;
GO
create schema silver;
GO
create schema gold;
GO