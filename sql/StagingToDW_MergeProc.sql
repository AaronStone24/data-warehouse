DROP PROCEDURE IF EXISTS DW_Staging.getColumnNames
DROP PROCEDURE IF EXISTS DW_Staging.generate_conditions
DROP PROCEDURE IF EXISTS DW_Staging.generateTempTableColumnStrings
DROP PROCEDURE IF EXISTS DW_Staging.AutoSCD2

SELECT '0, Procedures dropped successfully!' AS [Output];

GO
CREATE PROCEDURE DW_Staging.getColumnNames
@table varchar(127),
@seperator varchar(10),
@prefix varchar(50) = '',
@includeSourceTableColumn bit = 0,
@out varchar(max) OUTPUT
AS
BEGIN
	DECLARE @schema varchar(max) = '', @dataType nvarchar(50) = ''
	SET @schema = SUBSTRING(@table, 1, CHARINDEX('.', @table)-1)
	SET @table = SUBSTRING(@table, CHARINDEX('.', @table)+1, LEN(@table)-CHARINDEX('.', @table))
	
	DECLARE cs CURSOR FOR 
	SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS 
	WHERE UPPER(TABLE_NAME) = UPPER(@table) AND
		UPPER(TABLE_SCHEMA) = UPPER(@schema)

	OPEN cs;

	DECLARE @columnName varchar(50) = ''

	FETCH NEXT FROM cs INTO @columnName, @dataType
	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF (UPPER(@columnName) NOT IN (UPPER('loadTimeDate'), UPPER('endTimeDate'), UPPER('SourceTable')) AND
			COLUMNPROPERTY(OBJECT_ID(@schema + '.' + @table), @columnName, 'IsIdentity') != 1) OR
			(UPPER(@columnName) = UPPER('SourceTable') AND @includeSourceTableColumn = 1)
		BEGIN
			SET @out = CONCAT(@out, @prefix, @columnName, @seperator)
		END
		FETCH NEXT FROM cs INTO @columnName, @dataType
	END

	SET @out = SUBSTRING(@out, 1, LEN(@out)-LEN(@seperator))
	--PRINT @out

	CLOSE cs;
	DEALLOCATE cs;
END

/*
DROP PROCEDURE getColumnNames;
DECLARE @out VARCHAR(MAX)
EXEC getColumnNames @table='DW.Categories_Dim', @seperator=' , ', @includeSourceTableColumn = 1, @out=@out OUTPUT
SELECT @out
*/

---------------------------------------------------------------------------------------------------------------

GO
CREATE PROCEDURE DW_Staging.generate_conditions
@table varchar(127),
@operator varchar(5),
@seperator varchar(10),
@condition varchar(max) OUTPUT
AS
BEGIN
	DECLARE @schema varchar(max) = ''
	SET @schema = SUBSTRING(@table, 1, CHARINDEX('.', @table)-1)
	SET @table = SUBSTRING(@table, CHARINDEX('.', @table)+1, LEN(@table)-CHARINDEX('.', @table))
	
	DECLARE cs CURSOR FOR 
	SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS 
	WHERE UPPER(TABLE_NAME) = UPPER(@table) AND
		UPPER(TABLE_SCHEMA) = UPPER(@schema)

	OPEN cs;

	DECLARE @columnName varchar(50) = '', @dataType nvarchar(50) = ''

	FETCH NEXT FROM cs INTO @columnName, @dataType
	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF COLUMNPROPERTY(OBJECT_ID(@schema + '.' + @table), @columnName, 'IsIdentity') != 1 AND 
			UPPER(@columnName) NOT IN (UPPER('SourceTable'), UPPER('loadTimeDate'), UPPER('endTimeDate'))
			IF @dataType IS NOT NULL AND UPPER(@dataType) = UPPER('ntext')
				SET @condition = CONCAT(@condition, 'cast( TGT.' + @columnName + ' as nvarchar(max)) ', @operator, ' cast( SRC.' + @columnName + ' as nvarchar(max)) ', @seperator)
			ELSE
				SET @condition = CONCAT(@condition, ' TGT.', @columnName, ' ', @operator, ' SRC.', @columnName, ' ', @seperator)

		FETCH NEXT FROM cs INTO @columnName, @dataType
	END

	SET @condition = SUBSTRING(@condition, 1, LEN(@condition)-LEN(@seperator))

	CLOSE cs;
	DEALLOCATE cs;
END

/*
DROP PROCEDURE generate_conditions;
DECLARE @condition varchar(max)
EXEC generate_conditions @table='DW.Categories_Dim', @operator='!=', @seperator=' OR ', @condition=@condition OUTPUT
SELECT @condition;
*/

---------------------------------------------------------------------------------------------------------------

GO
CREATE PROCEDURE DW_Staging.generateTempTableColumnStrings
@referenceTable varchar(127),
@seperator varchar(5),
@out varchar(max) OUTPUT
AS
BEGIN
	DECLARE @schema varchar(max) = ''
	SET @schema = SUBSTRING(@referenceTable, 1, CHARINDEX('.', @referenceTable)-1)
	SET @referenceTable = SUBSTRING(@referenceTable, CHARINDEX('.', @referenceTable)+1, LEN(@referenceTable)-CHARINDEX('.', @referenceTable))

	DECLARE cs CURSOR FOR 
	SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS 
	WHERE UPPER(TABLE_NAME) = UPPER(@referenceTable) AND
		UPPER(TABLE_SCHEMA) = UPPER(@schema)

	OPEN cs;

	DECLARE @columnName varchar(50) = '', @dataType varchar(50) = '', @charMaxLength int = 0

	FETCH NEXT FROM cs INTO @columnName, @dataType, @charMaxLength
	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF COLUMNPROPERTY(OBJECT_ID(@schema + '.' + @referenceTable), @columnName, 'IsIdentity') != 1
		BEGIN
			IF (UPPER(@dataType) = UPPER('varchar') OR UPPER(@dataType) = UPPER('nvarchar') OR
				UPPER(@dataType) = UPPER('char') OR UPPER(@dataType) = UPPER('nchar')) AND
				@charMaxLength IS NOT NULL
					IF @charMaxLength = -1
						SET @dataType = CONCAT(@dataType, '(max)')
					ELSE
						SET @dataType = CONCAT(@dataType, '(', @charMaxLength, ')')
			
			IF UPPER(@columnName) = UPPER('loadTimeDate')
				SET @out = CONCAT(@out, @columnName, ' ', @dataType, ' DEFAULT CURRENT_TIMESTAMP', @seperator)
			ELSE
				SET @out = CONCAT(@out, @columnName, ' ', @dataType, @seperator)
		END
		FETCH NEXT FROM cs INTO @columnName, @dataType, @charMaxLength
	END

	SET @out = SUBSTRING(@out, 1, LEN(@out)-LEN(@seperator))

	CLOSE cs;
	DEALLOCATE cs;
END

/*
DECLARE @out varchar(max)
EXEC DW_Staging.generateTempTableColumnStrings @referenceTable='DW.Categories_Dim', @seperator=' , ', @out=@out OUTPUT
SELECT @out;
DROP PROCEDURE DW_Staging.generateTempTableColumnStrings;
*/

---------------------------------------------------------------------------------------------------------------

GO
CREATE PROCEDURE DW_Staging.AutoSCD2
@SourceTable varchar(127),
@TargetTable varchar(127),
@matching_condition varchar(127)
AS
SET NOCOUNT ON
BEGIN
	BEGIN TRY
		DECLARE @query varchar(max), @check_cond varchar(max), @query2 varchar(max), @src_table_columns varchar(max),
		@tgt_table_columns_wsc varchar(max), @src_table_prefix_columns varchar(max), @src_table_prefix_columns_wsc varchar(max),
		@tgt_table_columns varchar(max)
		DECLARE @result varchar(max) = ''

		EXEC DW_Staging.generate_conditions @table=@SourceTable, @operator='!=', @seperator='OR ', @condition=@check_cond OUTPUT
		EXEC DW_Staging.getColumnNames @table=@SourceTable, @seperator=' , ', @out=@src_table_columns OUTPUT
		EXEC DW_Staging.getColumnNames @table=@TargetTable, @seperator=' , ', @out=@tgt_table_columns OUTPUT
		EXEC DW_Staging.getColumnNames @table=@TargetTable, @seperator=' , ', @includeSourceTableColumn = 1, @out=@tgt_table_columns_wsc OUTPUT
		EXEC DW_Staging.getColumnNames @table=@SourceTable, @seperator=' , ', @prefix='SRC.', @out=@src_table_prefix_columns OUTPUT
		EXEC DW_Staging.getColumnNames @table=@SourceTable, @seperator=' , ', @prefix='SRC.', @includeSourceTableColumn = 1, @out=@src_table_prefix_columns_wsc OUTPUT

		DECLARE @tempTableName varchar(127) = 'tempTable', @tempTableColumns varchar(max) = ''

		EXEC DW_Staging.generateTempTableColumnStrings @referenceTable=@TargetTable, @seperator=' , ', @out=@tempTableColumns OUTPUT

		SET @query = 'DECLARE @' + @tempTableName + ' TABLE (' + @tempTableColumns + ')' + CHAR(13)

		SET @query = CONCAT(
			@query,
			'INSERT INTO @' + @tempTableName + ' (' + @tgt_table_columns_wsc + ')' + CHAR(13) +
			'SELECT ' + @tgt_table_columns + ', ' + '''' + @SourceTable + '''' + CHAR(13) +
			'FROM (' + CHAR(13) +
			'MERGE ' + @TargetTable + ' AS TGT' + CHAR(13) + 
			'USING ' + @SourceTable + ' AS SRC' + CHAR(13) + 
			'ON ' + @matching_condition + CHAR(13) + 
			'WHEN MATCHED AND' + CHAR(13) +
			'(' + @check_cond + ') AND TGT.endTimeDate IS NULL' + CHAR(13) +
			'THEN UPDATE' + CHAR(13) +
			'SET ' + 'TGT.endTimeDate = GetDate(), TGT.SourceTable = ' + '''' + @SourceTable + '''' + CHAR(13) +
			CHAR(13) +
			'WHEN NOT MATCHED BY TARGET' + CHAR(13) +
			'THEN INSERT ' + '(' + @src_table_columns + ', SourceTable)' + CHAR(13) +
			'VALUES (' + @src_table_prefix_columns + ', ' + '''' + @SourceTable + '''' + ')' + CHAR(13) +
			'OUTPUT $action, ' + @src_table_prefix_columns + CHAR(13) +
			') AS MERGE_OUT (' + CHAR(13) +
			'action, ' + @tgt_table_columns + ')' + CHAR(13) +
			'WHERE action = ' + '''' + 'UPDATE' + '''' + ';' + CHAR(13)
		)

		SET @query = CONCAT(
			@query,
			'INSERT INTO ' + @TargetTable + ' (' + @tgt_table_columns + ', loadTimeDate, endTimeDate, SourceTable)' + CHAR(13) +
			'SELECT *' +  + CHAR(13) +
			'FROM @' + @tempTableName + ';' + CHAR(13)
		)
	
		--PRINT @query

		EXEC(@query)
		
		SET @result = '0, Procedure AutoSCD2 executed successfully!'
	END TRY

	BEGIN CATCH
		SET @result = CAST(ERROR_NUMBER() AS varchar(10)) + ', Error: ' + ERROR_MESSAGE()
	END CATCH
	SELECT @result AS the_output;
END

GO
SELECT '0, Procedures created successfully!' AS [Output]

-- TODO: Delete the code below later
/*
DECLARE @tempTable TABLE (
	CategoryID INT,
	CategoryName VARCHAR(50),
	CatDescription NTEXT,
	loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
	endTimeDate DATETIME,
	SourceTable VARCHAR(MAX)
)

INSERT INTO @tempTable (CategoryID, CategoryName, CatDescription, SourceTable)
SELECT CategoryID, CategoryName, CatDescription, 'DW_Staging.Categories_Dim'
FROM
(
	MERGE DW.Categories_Dim AS TGT
	USING DW_Staging.Categories_Dim AS SRC
	ON TGT.CategoryID = SRC.categoryID
	WHEN MATCHED AND ((
	  --TGT.loadTimeDate <> SRC.loadTimeDate OR
		TGT.CategoryID <> SRC.CategoryID OR
		TGT.CategoryName <> SRC.CategoryName OR
		CAST(TGT.CatDescription AS nvarchar(max)) <> cast(SRC.CatDescription AS nvarchar(max))
	) AND TGT.endTimeDate IS NULL)
	THEN
	UPDATE SET TGT.endTimeDate = GetDate(),
		TGT.SourceTable = 'DW_Staging.Categories_Dim'

	WHEN NOT MATCHED BY TARGET THEN
	INSERT (CategoryID, CategoryName, CatDescription, SourceTable)
		VALUES (SRC.CategoryID, SRC.CategoryName, SRC.CatDescription, 'DW_Staging.Categories_Dim' )
	OUTPUT $action, SRC.CategoryID, SRC.CategoryName, SRC.CatDescription
) AS MERGE_OUT (
	action,
	CategoryID,
	CategoryName,
	CatDescription
)
WHERE action = 'UPDATE';

SELECT * FROM @tempTable
INSERT INTO DW.Categories_Dim (CategoryID, CategoryName, CatDescription, loadTimeDate, endTimeDate, SourceTable)
SELECT * FROM @tempTable

SELECT * FROM DW_Staging.Categories_Dim
SELECT * FROM DW.Categories_Dim

DROP TABLE DW.CustomerEmployee_Fact;
DROP TABLE DW.ProductInStock_Fact;
TRUNCATE TABLE DW.Categories_Dim

UPDATE DW_Staging.Categories_Dim
SET CategoryName = 'Beverages'
WHERE CategoryID = 1
UPDATE DW_Staging.Categories_Dim
SET CategoryName = 'Condiments'
WHERE CategoryID = 2

EXEC DW_Staging.AutoSCD2 @SourceTable='DW_Staging.Categories_Dim', @TargetTable='DW.Categories_Dim', @matching_condition='TGT.CategoryID=SRC.CategoryID'
DROP PROCEDURE DW_Staging.AutoSCD2;
*/

/*
SELECT * FROM DW_Staging.Customer_Dim
WHERE CustomerID = 'ALFKI'
SELECT * FROM DW.Customer_Dim
WHERE CustomerID = 'ALFKI'
TRUNCATE TABLE DW.Customer_Dim

EXEC DW_Staging.AutoSCD2 @SourceTable='DW_Staging.Customer_Dim', @TargetTable='DW.Customer_Dim', @matching_condition='TGT.CustomerID=SRC.CustomerID'
DROP PROCEDURE DW_Staging.AutoSCD2

UPDATE DW_Staging.Customer_Dim
SET Country = 'Deutschland/Germany'
WHERE CustomerID = 'ALFKI'

SELECT * FROM DW_Staging.Product_Dim
WHERE ProductID = 1
SELECT * FROM DW.Product_Dim
WHERE ProductID = 1
TRUNCATE TABLE DW.Product_Dim

EXEC DW_Staging.AutoSCD2 @SourceTable='DW_Staging.Product_Dim', @TargetTable='DW.Product_Dim', @matching_condition='TGT.ProductID=SRC.ProductID'

UPDATE DW_Staging.Product_Dim
SET UnitPrice = 20
WHERE ProductID = 1

TRUNCATE TABLE DW_Landing.Product_Dim
TRUNCATE TABLE DW_Landing.Calendar_Dim
TRUNCATE TABLE DW_Landing.Categories_Dim
TRUNCATE TABLE DW_Landing.Employee_Dim
TRUNCATE TABLE DW_Landing.Customer_Dim
TRUNCATE TABLE DW_Landing.Supplier_Dim

TRUNCATE TABLE DW_Staging.Product_Dim
TRUNCATE TABLE DW_Staging.Calendar_Dim
TRUNCATE TABLE DW_Staging.Categories_Dim
TRUNCATE TABLE DW_Staging.Employee_Dim
TRUNCATE TABLE DW_Staging.Customer_Dim
TRUNCATE TABLE DW_Staging.Supplier_Dim
*/