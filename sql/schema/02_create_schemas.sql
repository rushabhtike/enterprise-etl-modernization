USE retail_db;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'master_data')
BEGIN
    EXEC('CREATE SCHEMA master_data');
END

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'sales')
BEGIN
    EXEC('CREATE SCHEMA sales');
END

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'inventory')
BEGIN
    EXEC('CREATE SCHEMA inventory');
END

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'hr')
BEGIN
    EXEC('CREATE SCHEMA hr');
END

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'audit')
BEGIN
    EXEC('CREATE SCHEMA audit');
END