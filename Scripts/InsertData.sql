USE tempdb

DECLARE @UniqueCountriesCount INT

IF OBJECT_ID('tempdb..#CountryNames') IS NOT NULL
BEGIN
	DROP TABLE #CountryNames
END

SELECT DISTINCT(Country) AS CountryName,
	ROW_NUMBER() OVER (ORDER BY Country) AS RowNum
INTO #CountryNames
FROM Tempdb..Initial_Ingest

SET @UniqueCountriesCount = @@ROWCOUNT

SELECT * from #CountryNames
--SELECT @UniqueCountries

BEGIN TRY

	WHILE(@UniqueCountriesCount>0)
	BEGIN
		DECLARE @tableName NVARCHAR(255)
		DECLARE @country NVARCHAR(50)
	
		SELECT @country = CountryName
		FROM #CountryNames
		WHERE RowNum = @UniqueCountriesCount

		SET @tableName = N'Table_' + @country

		IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @tableName)
		BEGIN
			DECLARE @sql NVARCHAR(MAX)

			SET @sql = 'CREATE TABLE ' + @tableName +
			' ( [Customer_Name] [varchar](255) NOT NULL,
				[Customer_Id] [varchar](18) NOT NULL,
				[Open_Date] [varchar](50) NOT NULL,
				[Last_Consulted_Date] [varchar](50) NULL,
				[Vaccination_Id] [varchar](5) NULL,
				[Dr_Name] [varchar](255) NULL,
				[State] [varchar](5) NULL,
				[Country] [varchar](5) NULL,
				[Post_Code] [numeric](18, 0) NULL,
				[DOB] [varchar](50) NULL,
				[Is_Active] [varchar](1) NULL
			)'

			EXEC (@sql)
		END

		SET @sql = NULL

		SET @sql = 'INSERT INTO ' + @tableName +
					' SELECT *	
					FROM Initial_Ingest
					WHERE Country = ''' + @country + ''''
		SELECT @sql

		EXEC (@sql)

		SET @UniqueCountriesCount = @UniqueCountriesCount - 1
	END
END TRY
BEGIN CATCH
	SELECT  
    ERROR_NUMBER() AS ErrorNumber  
    ,ERROR_SEVERITY() AS ErrorSeverity  
    ,ERROR_STATE() AS ErrorState  
    ,ERROR_PROCEDURE() AS ErrorProcedure  
    ,ERROR_LINE() AS ErrorLine  
    ,ERROR_MESSAGE() AS ErrorMessage
END CATCH
