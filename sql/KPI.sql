-- KPI 1 -- Discontinued product report
SELECT PD.ProductID, PD.ProductName, PD.UnitPrice 
FROM DW.Product_Dim AS PD
where PD.Discontinued = 1;

-- KPI 2 -- Best and least selling product in each category
SELECT A.CategoryName, [BestSellingProduct], [HighestSalesInCategory], [LeastSellingProduct], [LowestSalesInCategory]
FROM (
	SELECT CD.CategoryName, ProductName AS [BestSellingProduct], PIS.TotalQuantity AS [HighestSalesInCategory],
	ROW_NUMBER() OVER (PARTITION BY CD.CategoryName ORDER BY CD.CategoryName) AS [RowNumber]
	FROM DW.Product_Dim PD
	JOIN DW_Landing.ProductInStock_Fact PIS
	ON PD.ProductKey = PIS.ProductKey
	JOIN DW.Categories_Dim CD
	ON CD.CategoriesKey = PIS.CategoriesKey
	WHERE PD.ProductKey = (
		SELECT TOP 1 ProductKey
		FROM DW_Landing.ProductInStock_Fact
		WHERE TotalQuantity = (SELECT MAX(TotalQuantity) FROM DW_Landing.ProductInStock_Fact
			WHERE CategoriesKey = CD.CategoriesKey)
	) 
) A,
(
	SELECT CD.CategoryName, ProductName AS [LeastSellingProduct], PIS.TotalQuantity AS [LowestSalesInCategory],
	ROW_NUMBER() OVER (PARTITION BY CD.CategoryName ORDER BY CD.CategoryName) AS [RowNumber]
	FROM DW.Product_Dim PD
	JOIN DW_Landing.ProductInStock_Fact PIS
	ON PD.ProductKey = PIS.ProductKey
	JOIN DW.Categories_Dim CD
	ON CD.CategoriesKey = PIS.CategoriesKey
	WHERE PD.ProductKey = (
		SELECT TOP 1 ProductKey
		FROM DW_Landing.ProductInStock_Fact
		WHERE TotalQuantity = (SELECT MIN(TotalQuantity) FROM DW_Landing.ProductInStock_Fact
			WHERE CategoriesKey = CD.CategoriesKey)
	) 
) B
WHERE A.CategoryName = B.CategoryName AND A.RowNumber = 1 AND B.RowNumber = 1

--KPI 3 -- min max avg customer billing
SELECT CD.CustomerID, MIN(Sales) AS [Minimum Sales],
MAX(Sales) AS [Maximum Sales],
AVG(Sales) AS [Average Sales]
FROM DW_Landing.CustomerEmployee_Fact AS CF
JOIN DW.Customer_Dim AS CD
ON CF.CustomerKey = CD.CustomerKey
GROUP BY CD.CustomerID

-- KPI 4 -- Best Salesperson
SELECT TOP 1 EmployeeID, FirstName, LastName, TotalSales
FROM 
(
	SELECT ED.EmployeeID, ED.FirstName, ED.LastName,
		SUM(Sales) AS TotalSales
	FROM DW_Landing.CustomerEmployee_Fact AS CF
	JOIN DW.Employee_Dim AS ED
	ON CF.EmployeeKey = ED.EmployeeKey
	GROUP BY ED.EmployeeID, FirstName, LastName
) A
ORDER BY TotalSales DESC;