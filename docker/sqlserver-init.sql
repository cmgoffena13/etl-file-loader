-- Create the fileloader database if it doesn't exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'fileloader')
BEGIN
    CREATE DATABASE fileloader;
    PRINT 'Database fileloader created successfully.';
END
ELSE
BEGIN
    PRINT 'Database fileloader already exists.';
END
GO
