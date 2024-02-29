--drop database dw_landing
create schema DW
 
create table DW.Categories_Dim(
CategoriesKey int Primary Key identity,
CategoryID int,
CategoryName varchar(50),
CatDescription ntext,
loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
endTimeDate DATETIME DEFAULT NULL,
SourceTable varchar(max)
)--done

/*
INSERT INTO DW.Categories_Dim (CategoryID, CategoryName, CatDescription, SourceTable)
VALUES (123, 'Toys', 'Optimus Prime Toy', 'dbo.Categories')
drop table DW.Categories_Dim
SELECT COLUMNPROPERTY(OBJECT_ID('DW_Landing.Categories_Dim'), 'CategoriesKey', 'IsIdentity')
 */

create table DW.Product_Dim(
ProductKey int Primary Key identity,
ProductID varchar(5),
ProductName varchar(100),
UnitPrice int ,
Discontinued int,
loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
endTimeDate DATETIME DEFAULT NULL,
SourceTable varchar(max)
)--done
 
create table DW.Supplier_Dim(
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
endTimeDate DATETIME DEFAULT NULL,
SourceTable varchar(max)
)--done
 
create table DW.Calendar_Dim(
CalendarKey int primary key identity,
FullDate datetime unique,
DayofTheWeek varchar(max),
DayType varchar(max),
DayofTheMonth int,
[Month] varchar(max),
[Quarter] varchar(max),
[Year] int,
loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
endTimeDate DATETIME DEFAULT NULL,
SourceTable varchar(max)
);--done
--drop table DW.Calendar_Dim
 
create table DW.Customer_Dim(
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
endTimeDate DATETIME DEFAULT NULL,
SourceTable varchar(max)
);--done
--drop table Customer_Dim
 
create table DW.Employee_Dim(
EmployeeKey int primary key identity,
EmployeeID int,
LastName varchar(20),
FirstName varchar(10),
BirthDate datetime,
HireDate datetime,
Region varchar(15),
Country varchar(15),
loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
endTimeDate DATETIME DEFAULT NULL,
SourceTable varchar(max)
);--done
--drop table Employee_Dim
 
create table DW.CustomerEmployee_Fact(
CustomerKey int foreign key references DW.Customer_Dim(customerkey),
EmployeeKey int foreign key references DW.Employee_Dim(Employeekey),
CalendarKey int foreign key references DW.Calendar_Dim(CalendarKey),
OrderId varchar(max),
Sales money,
loadTimeDate DATETIME DEFAULT CURRENT_TIMESTAMP,
SourceTable varchar(max)
);--done
--drop table DW.CustomerEmployee_Fact
 
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
)
--drop table DW.ProductInStock_Fact
--waiting for other tables
