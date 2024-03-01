BEGIN TRY
    DECLARE @result varchar(max) = ''

    IF NOT EXISTS (SELECT TOP 1 * FROM sys.schemas WHERE name = 'DW')
    BEGIN
        EXEC('CREATE SCHEMA DW')
    END
    ELSE
    BEGIN
        DROP TABLE IF EXISTS DW.CustomerEmployee_Fact
        DROP TABLE IF EXISTS DW.ProductInStock_Fact
    END
   
    create table DW.CustomerEmployee_Fact(
    CustomerKey int foreign key references DW.Customer_Dim(customerkey),
    EmployeeKey int foreign key references DW.Employee_Dim(Employeekey),
    CalendarKey int foreign key references DW.Calendar_Dim(CalendarKey),
    OrderId varchar(max),
    Sales money,
    loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
    SourceTable varchar(max)
    );
    
    create table DW.ProductInStock_Fact(
    Calendarkey int foreign key references DW.Calendar_Dim(CalendarKey),
    ProductKey int foreign key references DW.Product_dim(productKey),
    CategoriesKey int foreign key references DW.Categories_dim(categoriesKey),
    SupplierKey int foreign key references DW.supplier_dim(supplierKey),
    UnitsInStock int,
    UnitsOnOrder int,
    ReorderLevel int,
    TotalQuantity int,
    OrderId varchar(20),
    loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
    SourceTable varchar(max)
    );

    SET @result = '0, DW tables created successfully!'
END TRY
BEGIN CATCH
    SET @result = CAST(ERROR_NUMBER() AS varchar(10)) + ', Error: ' + ERROR_MESSAGE()
END CATCH

SELECT @result AS [Output];