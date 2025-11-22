-- Create the fileloader database if it doesn't exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'fileloader')
BEGIN
    CREATE DATABASE fileloader;
    ALTER DATABASE fileloader SET RECOVERY SIMPLE;
    PRINT 'Database fileloader created successfully with minimal logging.';
END
ELSE
BEGIN
    -- Ensure minimal logging is enabled even if database already exists
    ALTER DATABASE fileloader SET RECOVERY SIMPLE;
    PRINT 'Database fileloader already exists. Minimal logging enabled.';
END
GO
