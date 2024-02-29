BEGIN TRY
    DECLARE @result varchar(max) = ''

    IF NOT EXISTS (SELECT TOP 1 * FROM sys.schemas WHERE name = 'DW_Landing')
    BEGIN
        EXEC('CREATE SCHEMA DW_Landing')
    END
    ELSE
    BEGIN
    --Todo: REMOVE FACT TABLES FROM HERE AND ADD BRIDGE TABLES
        DROP TABLE IF EXISTS DW_Landing.CustomerEmployee_Fact
        DROP TABLE IF EXISTS DW_Landing.ProductInStock_Fact
        DROP TABLE IF EXISTS DW_Landing.Categories_Dim
        DROP TABLE IF EXISTS DW_Landing.Product_Dim
        DROP TABLE IF EXISTS DW_Landing.Supplier_Dim
        DROP TABLE IF EXISTS DW_Landing.Calendar_Dim
        DROP TABLE IF EXISTS DW_Landing.Customer_Dim
        DROP TABLE IF EXISTS DW_Landing.Employee_Dim
    END


    create table DW_Landing.Categories_Dim(
    CategoriesKey int Primary Key identity,
    CategoryID int ,
    CategoryName varchar(50),
    CatDescription ntext,
    loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
    SourceTable varchar(max)
    )
    
    create table DW_Landing.Product_Dim(
    ProductKey int Primary Key identity,
    ProductID varchar(5),
    ProductName varchar(100),
    UnitPrice int ,
    Discontinued int,
    loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
    SourceTable varchar(max)
    )
    
    create table DW_Landing.Supplier_Dim(
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
    
    create table DW_Landing.Calendar_Dim(
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
    
    create table DW_Landing.Customer_Dim(
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
    
    create table DW_Landing.Employee_Dim(
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
    
    /*
    create table DW_Landing.CustomerEmployee_Fact(
    CustomerKey int foreign key references DW_Landing.Customer_Dim(customerkey),
    EmployeeKey int foreign key references DW_Landing.Employee_Dim(Employeekey),
    CalendarKey int foreign key references DW_Landing.Calendar_Dim(CalendarKey),
    OrderId varchar(max),
    Sales money,
    loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
    SourceTable varchar(max)
    );
    
    create table DW_Landing.ProductInStock_Fact(
    Calendarkey int foreign key references DW_Landing.Calendar_Dim(CalendarKey),
    ProductKey int foreign key references DW_Landing.Product_dim(productKey),
    CategoriesKey int foreign key references DW_Landing.Categories_dim(categoriesKey),
    SupplierKey int foreign key references DW_Landing.supplier_dim(supplierKey),
    UnitsInStock int,
    UnitsOnOrder int,
    ReorderLevel int,
    TotalQuantity int,
    OrderId varchar(20),
    loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
    SourceTable varchar(max)
    )
    */

    SET @result = '0, Landing tables created successfully!'
END TRY
BEGIN CATCH
    SET @result = CAST(ERROR_NUMBER() AS varchar(10)) + ', Error: ' + ERROR_MESSAGE()
END CATCH

SELECT @result AS [Output];