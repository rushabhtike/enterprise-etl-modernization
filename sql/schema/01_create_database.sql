USE master;

IF DB_ID('retail_db') IS NULL
BEGIN
    CREATE DATABASE retail_db;
END
