/*
	MERGE HumanResources.Dept AS TGT
	USING HumanResources.Department AS SRC
	ON TGT.DepartmentID = SRC.DepartmentID

	WHEN MATCHED AND
	(TGT.Name != SRC.Name OR
	TGT.GroupName != SRC.GroupName OR
	TGT.ModifiedDate != SRC.ModifiedDate)
	THEN UPDATE
	SET TGT.Name = SRC.Name,
	TGT.GroupName = SRC.GroupName,
	TGT.ModifiedDate = SRC.ModifiedDate

	WHEN NOT MATCHED
	THEN INSERT (DepartmentID, Name, GroupName, ModifiedDate)
		VALUES (SRC.DepartmentID, SRC.Name, SRC.GroupName, SRC.ModifiedDate);
*/

------------------------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS DW_Landing.getColumnNames
DROP PROCEDURE IF EXISTS DW_Landing.generate_conditions
DROP PROCEDURE IF EXISTS DW_Landing.AutoSCD1

GO
CREATE PROCEDURE DW_Landing.getColumnNames
@table varchar(127),
@seperator varchar(10),
@prefix varchar(50) = '',
@out varchar(max) OUTPUT
AS
BEGIN
	DECLARE @schema varchar(max) = ''
	SET @schema = SUBSTRING(@table, 1, CHARINDEX('.', @table)-1)
	SET @table = SUBSTRING(@table, CHARINDEX('.', @table)+1, LEN(@table)-CHARINDEX('.', @table))
	
	DECLARE cs CURSOR FOR 
	SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
	WHERE UPPER(TABLE_NAME) = UPPER(@table) AND
		UPPER(TABLE_SCHEMA) = UPPER(@schema)

	OPEN cs;

	DECLARE @columnName varchar(50) = ''

	FETCH NEXT FROM cs INTO @columnName
	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF UPPER(@columnName) != UPPER('loadTimeDate') AND UPPER(@columnName) != UPPER('SourceTable') AND
			COLUMNPROPERTY(OBJECT_ID(@schema + '.' + @table), @columnName, 'IsIdentity') != 1
		BEGIN
			SET @out = CONCAT(@out, @prefix, @columnName, @seperator)
		END
		FETCH NEXT FROM cs INTO @columnName
	END

	SET @out = SUBSTRING(@out, 1, LEN(@out)-LEN(@seperator))

	CLOSE cs;
	DEALLOCATE cs;
END

-------------------------------------------------------------------------------------------

GO
CREATE PROCEDURE DW_Landing.generate_conditions
@table varchar(127),
@operator varchar(5),
@seperator varchar(10),
@isSetQuery bit = 1,
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
			UPPER(@columnName) != UPPER('SourceTable') AND (UPPER(@columnName) != UPPER('loadTimeDate') OR @isSetQuery = 1)
			IF @isSetQuery = 0 AND @dataType IS NOT NULL AND UPPER(@dataType) = UPPER('ntext')
				SET @condition = CONCAT(@condition, 'cast( TGT.' + @columnName + ' as nvarchar(max)) ', @operator, ' cast( SRC.' + @columnName + ' as nvarchar(max)) ', @seperator)
			ELSE
				SET @condition = CONCAT(@condition, ' TGT.', @columnName, ' ', @operator, ' SRC.', @columnName, ' ', @seperator)

		FETCH NEXT FROM cs INTO @columnName, @dataType
	END

	SET @condition = SUBSTRING(@condition, 1, LEN(@condition)-LEN(@seperator))

	CLOSE cs;
	DEALLOCATE cs;
END

----------------------------------------------------------------------------------------------

GO
CREATE PROCEDURE DW_Landing.AutoSCD1
@SourceTable varchar(127),
@TargetTable varchar(127),
@matching_condition varchar(127)
AS
SET NOCOUNT ON
BEGIN
	BEGIN TRY
		DECLARE @query varchar(max), @cond1 varchar(max), @query2 varchar(max), @columns varchar(max),
		@src_columns varchar(max)

		EXEC DW_Landing.generate_conditions @table=@SourceTable, @operator='!=', @seperator='OR ', @isSetQuery = 0, @condition=@cond1 OUTPUT
		EXEC DW_Landing.generate_conditions @table=@SourceTable, @operator='=', @seperator=' , ', @condition=@query2 OUTPUT
		EXEC DW_Landing.getColumnNames @table=@SourceTable, @seperator=' , ', @out=@columns OUTPUT
		EXEC DW_Landing.getColumnNames @table=@SourceTable, @seperator=' , ', @prefix='SRC.', @out=@src_columns OUTPUT

		SET @query = 'MERGE ' + @TargetTable + ' AS TGT' + CHAR(13) + 
			'USING ' + @SourceTable + ' AS SRC' + CHAR(13) + 
			'ON ' + @matching_condition + CHAR(13) + 
			'WHEN MATCHED AND' + CHAR(13) +
			'(' + @cond1 + ')' + CHAR(13) +
			'THEN UPDATE' + CHAR(13) +
			'SET ' + @query2 + CHAR(13) +
			CHAR(13) +
			'WHEN NOT MATCHED BY TARGET' + CHAR(13) +
			'THEN INSERT ' + '(' + @columns + ', SourceTable)' + CHAR(13) +
			'VALUES (' + @src_columns + ', ' + '''' + @SourceTable + '''' + ');'
	
		EXEC(@query)
		
		DECLARE @result varchar(max) = ''
		SET @result = '0, Procedure AutoSCD1 executed successfully!'
	END TRY

	BEGIN CATCH
		SET @result = CAST(ERROR_NUMBER() AS varchar(10)) + ', Error: ' + ERROR_MESSAGE()
	END CATCH
	SELECT @result AS the_output;
END

/*
DROP PROCEDURE AutoSCD1;
EXEC AutoSCD1 @SourceTable='DW_Landing.Categories_Dim', @TargetTable='DW_Staging.Categories_Dim', @matching_condition='TGT.CategoriesKey=SRC.CategoriesKey'

INSERT INTO DW_Landing.Categories_Dim (CategoryID, CategoryName, CatDescription, SourceTable)
VALUES (123, 'Toys', 'Optimus Prime Toy', 'dbo.Categories')

UPDATE DW_Landing.Categories_Dim 
SET CategoryName = 'New Toys'
WHERE CategoriesKey = 2

SELECT * FROM DW_Staging.Categories_Dim
*/