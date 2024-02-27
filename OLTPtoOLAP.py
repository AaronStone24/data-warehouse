import pandas as pd
import numpy as np
import pyodbc as odbc
from sqlalchemy import create_engine

from Constants import CONNECTION_URL, SRC_SCHEMA, TGT_SCHEMA
from Mapper import oltp_to_olap_mapping, customerEmployee_Fact_mapper, ProductInStock_Fact_mapper

# TODO: Logging and Error Handling
# TODO: Consider using a configuration file for the connection strings
# TODO: Consider using SqlAlchemy's ORM based Tables

# Create a connection to the OLTP database
engine = create_engine(CONNECTION_URL)

# Read the data from the OLTP database by obtaining the connection
with engine.connect() as conn:
    # Mapping the OLTP tables to OLAP tables
    '''
    OLTP: Products(ProductID, ProductName, UnitPrice, Discontinued)
    OLAP: Product_Dim(ProductKey, ProductID, ProductName, UnitPrice, Discontinued)
    One to one mapping of columns from OLTP to OLAP
    '''
    oltp_to_olap_mapping(
        'Products',
        'Product_Dim', 
        {'ProductID': 'ProductID', 'ProductName': 'ProductName', 'UnitPrice': 'UnitPrice', 'Discontinued': 'Discontinued'},
        conn
    )

    '''
    OLTP: Suppliers(SupplierID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone)
    OLAP: Supplier_Dim(SupplierKey, SupplierID, CompanyName, ContactName, ContactTitle, SupplierAddress, City, Region, PostalCode, Country, Phone, loadTimeDate, SourceTable)
    '''
    oltp_to_olap_mapping(
        'Suppliers',
        'Supplier_Dim', 
        {'SupplierID': 'SupplierID', 'CompanyName': 'CompanyName', 'ContactName': 'ContactName', 'ContactTitle': 'ContactTitle', 'Address': 'SupplierAddress', 'City': 'City', 'Region': 'Region', 'PostalCode': 'PostalCode', 'Country': 'Country', 'Phone': 'Phone'},
        conn
    )

    '''
    OLTP: Categories(CategoryID, CategoryName, Description)
    OLAP: Category_Dim(CategoriesKey, CategoryID, CategoryName, CatDescription, loadTimeDate, SourceTable)
    '''
    oltp_to_olap_mapping(
        'Categories',
        'Categories_Dim', 
        {'CategoryID': 'CategoryID', 'CategoryName': 'CategoryName', 'Description': 'CatDescription'},
        conn
    )

    '''
    OLTP: Orders(OrderDate)
    OLAP: Calendar_Dim(CalendarKey, FullDate, DayofTheWeek, DayType, DayofTheMonth, Month, Quarter, Year, loadTimeDate, SourceTable)
    '''
    oltp_to_olap_mapping(
        'Orders',
        'Calendar_Dim', 
        {'OrderDate': 'FullDate'},
        conn
    )

    '''
    OLTP: Customers(CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone)
    OLAP: Customer_Dim(CustomerKey, CustomerID, CompanyName, ContactName, ContactTitle, CustAddress, City, Region, PostalCode, Country, Phone, loadTimeDate, SourceTable)
    '''
    oltp_to_olap_mapping(
        'Customers',
        'Customer_Dim', 
        {'CustomerID': 'CustomerID', 'CompanyName': 'CompanyName', 'ContactName': 'ContactName', 'ContactTitle': 'ContactTitle', 'Address': 'CustAddress', 'City': 'City', 'Region': 'Region', 'PostalCode': 'PostalCode', 'Country': 'Country', 'Phone': 'Phone'},
        conn
    )

    '''
    OLTP: Employees(EmployeeID, LastName, FirstName, BirthDate, HireDate, Region, Country)
    OLAP: Employee_Dim(EmployeeKey, EmployeeID, LastName, FirstName, BirthDate, HireDate, Region, Country, loadTimeDate, SourceTable)
    '''
    oltp_to_olap_mapping(
        'Employees',
        'Employee_Dim', 
        {'EmployeeID': 'EmployeeID', 'LastName': 'LastName', 'FirstName': 'FirstName', 'BirthDate': 'BirthDate', 'HireDate': 'HireDate', 'Region': 'Region', 'Country': 'Country'},
        conn
    )
    
    '''
    OLTP: Orders(OrderID), OrderDetails(OrderID, UnitPrice, Quantity, Discount)
    OLAP: CustomerEmployee_Fact(CustomerKey, EmployeeKey, CalendarKey, OrderID, Sales, loadTimeDate, SourceTable)
    '''
    customerEmployee_Fact_mapper(conn)

    '''
    OLTP: Orders(OrderID), Order Details(ProductID, Quantity), Products(UnitsInStock, UnitsOnOrder, ReorderLevel)
    OLAP: ProductInStock_Fact(CalendarKey, ProductKey, CategoriesKey, SupplierKey, UnitsInStock, UnitsOnOrder, ReorderLevel, TotalQuantity, OrderID, loadTimeDate, SourceTable)
    '''
    ProductInStock_Fact_mapper(conn)