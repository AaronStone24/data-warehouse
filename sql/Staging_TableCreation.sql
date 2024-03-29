BEGIN TRY
    DECLARE @result varchar(max) = ''

    IF NOT EXISTS (SELECT TOP 1 * FROM sys.schemas WHERE name = 'DW_Staging')
    BEGIN
        EXEC('CREATE SCHEMA DW_Staging')
    END
    ELSE
    BEGIN
        DROP TABLE IF EXISTS DW_Staging.CustomerEmployee_Bridge
        DROP TABLE IF EXISTS DW_Staging.ProductInStock_Bridge
        DROP TABLE IF EXISTS DW_Staging.Categories_Dim
        DROP TABLE IF EXISTS DW_Staging.Product_Dim
        DROP TABLE IF EXISTS DW_Staging.Supplier_Dim
        DROP TABLE IF EXISTS DW_Staging.Calendar_Dim
        DROP TABLE IF EXISTS DW_Staging.Customer_Dim
        DROP TABLE IF EXISTS DW_Staging.Employee_Dim
    END

    create table DW_Staging.Categories_Dim(
    CategoriesKey int Primary Key identity,
    CategoryID int ,
    CategoryName varchar(50),
    CatDescription ntext,
    loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
    SourceTable varchar(max)
    )
    
    create table DW_Staging.Product_Dim(
    ProductKey int Primary Key identity,
    ProductID varchar(5),
    ProductName varchar(100),
    UnitPrice int ,
    Discontinued int,
    loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
    SourceTable varchar(max)
    )
    
    create table DW_Staging.Supplier_Dim(
    SupplierKey int primary key identity,
    SupplierID int,
    CompanyName varchar(50),
    ContactName varchar(50),
    ContactTitle varchar(50),
    SupplierAddress varchar(50),
    City Varchar(50),
    Region varchar(50),
    PostalCode varchar(50),
    Country varchar(50),
    Phone varchar(50),
    loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
    SourceTable varchar(max)
    )
    
    create table DW_Staging.Calendar_Dim(
    CalendarKey int primary key identity,
    FullDate datetime unique,
    DayofTheWeek varchar(max),
    DayType varchar(max),
    DayofTheMonth int,
    [Month] varchar(max),
    [Quarter] varchar(max),
    [Year] int,
    loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
    SourceTable varchar(max)
    );
    
    create table DW_Staging.Customer_Dim(
    CustomerKey int primary key identity,
    CustomerID varchar(10),
    CompanyName varchar(50),
    ContactName varchar(50),
    ContactTitle varchar(50),
    CustAddress varchar(60),
    City varchar(20),
    Region varchar(20),
    PostalCode varchar(20),
    Country varchar(20),
    Phone varchar(20),
    loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
    SourceTable varchar(max)
    );
    
    create table DW_Staging.Employee_Dim(
    EmployeeKey int primary key identity,
    EmployeeID int,
    LastName varchar(20),
    FirstName varchar(10),
    BirthDate datetime,
    HireDate datetime,
    Region varchar(15),
    Country varchar(15),
    loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
    SourceTable varchar(max)
    );
    
    create table DW_Staging.CustomerEmployee_Bridge(
    CEBridgeKey int primary key identity,
    CustomerID varchar(10),
    EmployeeID int,
    OrderDate DATETIME,
    OrderId varchar(max),
    Sales money,
    loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
    SourceTable varchar(max)
    );
    
    create table DW_Staging.ProductInStock_Bridge(
    PISBridgeKey int primary key identity,
    ProductID varchar(5),
    CategoryID int,
    SupplierID int,
    OrderDate DATETIME,
    UnitsInStock int,
    UnitsOnOrder int,
    ReorderLevel int,
    TotalQuantity int,
    OrderId varchar(20),
    loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
    SourceTable varchar(max)
    );

    SET @result = '0, Staging tables created successfully!'
END TRY
BEGIN CATCH
    SET @result = CAST(ERROR_NUMBER() AS varchar(10)) + ', Error: ' + ERROR_MESSAGE()
END CATCH

SELECT @result AS [Output];