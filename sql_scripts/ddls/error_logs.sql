CREATE TABLE dbo.error_logs (
    id INT PRIMARY KEY NOT NULL,
    table_name NVARCHAR NULL,
    invalid_data NVARCHAR NULL,
    error_message NVARCHAR NULL,
    inserted_at DATETIME NULL
);