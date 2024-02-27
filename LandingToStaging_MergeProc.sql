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
CREATE PROCEDURE getColumnNames
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
	--PRINT @out

	CLOSE cs;
	DEALLOCATE cs;
END

DROP PROCEDURE getColumnNames;
DECLARE @out VARCHAR(MAX)
EXEC getColumnNames @table='DW_Landing.Categories_Dim', @seperator=' , ', @prefix='SRC.', @out=@out OUTPUT
SELECT @out

-------------------------------------------------------------------------------------------

CREATE PROCEDURE generate_conditions
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
	--SET @condition = '('

	FETCH NEXT FROM cs INTO @columnName, @dataType
	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF COLUMNPROPERTY(OBJECT_ID(@schema + '.' + @table), @columnName, 'IsIdentity') != 1 AND 
			UPPER(@columnName) != UPPER('SourceTable')
			IF @isSetQuery = 0 AND @dataType IS NOT NULL AND UPPER(@dataType) = UPPER('ntext')
				SET @condition = CONCAT(@condition, 'cast( TGT.' + @columnName + ' as nvarchar(max)) ', @operator, ' cast( SRC.' + @columnName + ' as nvarchar(max)) ', @seperator)
			ELSE
				SET @condition = CONCAT(@condition, ' TGT.', @columnName, ' ', @operator, ' SRC.', @columnName, ' ', @seperator)

		FETCH NEXT FROM cs INTO @columnName, @dataType
	END

	SET @condition = SUBSTRING(@condition, 1, LEN(@condition)-LEN(@seperator))
	--SET @condition = CONCAT(@condition, ')')
	--PRINT @condition

	CLOSE cs;
	DEALLOCATE cs;
END

DROP PROCEDURE generate_conditions;
DECLARE @condition varchar(max)
EXEC generate_conditions @table='DW_Landing.Categories_Dim', @operator='!=', @seperator=' OR ', @condition=@condition OUTPUT
SELECT @condition;

----------------------------------------------------------------------------------------------

CREATE PROCEDURE AutoSCD1 --could take more variables
@SourceTable varchar(127),
@TargetTable varchar(127),
@matching_condition varchar(127)
AS
BEGIN
	DECLARE @query varchar(max), @cond1 varchar(max), @query2 varchar(max), @columns varchar(max),
	@src_columns varchar(max), @src_table_name varchar(max)

	SET @src_table_name = SUBSTRING(@SourceTable, CHARINDEX('.', @SourceTable)+1, LEN(@SourceTable)-CHARINDEX('.', @SourceTable))
	--PRINT @src_table_name

	--DECLARE @ColumnNames = (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @SourceTable)
	--EXEC generate_matching_condition @table=@src_table_name, @condition=@matching_condition OUTPUT
	EXEC generate_conditions @table=@SourceTable, @operator='!=', @seperator='OR ', @isSetQuery = 0, @condition=@cond1 OUTPUT
	EXEC generate_conditions @table=@SourceTable, @operator='=', @seperator=' , ', @condition=@query2 OUTPUT
	EXEC getColumnNames @table=@SourceTable, @seperator=' , ', @out=@columns OUTPUT
	EXEC getColumnNames @table=@SourceTable, @seperator=' , ', @prefix='SRC.', @out=@src_columns OUTPUT

	SET @query = 'MERGE ' + @TargetTable + ' AS TGT' + CHAR(13) + 
		'USING ' + @SourceTable + ' AS SRC' + CHAR(13) + 
		'ON ' + @matching_condition + CHAR(13) + 
		'WHEN MATCHED AND' + CHAR(13) +
		'(' + @cond1 + ')' + CHAR(13) +
		'THEN UPDATE' + CHAR(13) +
		'SET ' + @query2 + CHAR(13) +
		CHAR(13) +
		'WHEN NOT MATCHED' + CHAR(13) +
		'THEN INSERT ' + '(' + @columns + ', SourceTable)' + CHAR(13) +
		'VALUES (' + @src_columns + ', ' + '''' + @SourceTable + '''' + ');'
	
	PRINT @query

	EXEC(@query)

	--SET @query = 'TRUNCATE TABLE ' + @SourceTable
	--EXEC(@query)

END

DROP PROCEDURE AutoSCD1;
EXEC AutoSCD1 @SourceTable='DW_Landing.Categories_Dim', @TargetTable='DW_Staging.Categories_Dim', @matching_condition='TGT.CategoriesKey=SRC.CategoriesKey'

INSERT INTO DW_Landing.Categories_Dim (CategoryID, CategoryName, CatDescription, SourceTable)
VALUES (123, 'Toys', 'Optimus Prime Toy', 'dbo.Categories')

UPDATE DW_Landing.Categories_Dim 
SET CategoryName = 'New Toys'
WHERE CategoriesKey = 2

SELECT * FROM DW_Staging.Categories_Dim