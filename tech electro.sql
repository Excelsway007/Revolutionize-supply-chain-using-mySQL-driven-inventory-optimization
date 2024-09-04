-- preliminaries - schema creation
CREATE SCHEMA tech_electro;
USE tech_electro;

-- Data exploration
SELECT * from external_factors limit 5;
SELECT * from sales_data limit 5;
SELECT * from Inventory_data limit 5;
SELECT * from product_information limit 5;

-- Understanding the structure of our dataset
show columns from external_factors;
DESC product_information;
DESC sales_data;

-- Data cleaning
-- changing to the right data type
-- external factors table
-- right data types for the columns Salesdata DATE, GDP DECIMAL(15,2), seasonalfactor DECIMAL(5,2), inflation rate DECIMAL(5,2)
ALTER TABLE external_factors
ADD COLUMN New_Sales_Date DATE;
SET SQL_SAFE_UPDATES=0; -- turning off safe update so we can alter the table
UPDATE external_factors
SET New_Sales_Date = STR_TO_DATE(Sales_Date, '%d/%m/%Y');
ALTER TABLE external_factors
DROP COLUMN Sales_Date;
ALTER TABLE external_factors
CHANGE COLUMN New_Sales_Date Sales_Date DATE;

ALTER TABLE external_factors
MODIFY COLUMN GDP DECIMAL(15,2);

ALTER TABLE external_factors
MODIFY COLUMN inflation_rate DECIMAL(5,2);

ALTER TABLE external_factors
MODIFY COLUMN seasonal_factor DECIMAL(5,2);

-- product data
-- product id INT, product category TEXT, promotions ENUM('yes','no')
ALTER TABLE product_information
ADD COLUMN NewPromotions ENUM('yes','no');
UPDATE product_information
SET NewPromotions = case when promotions = 'yes' then 'yes'
when promotions = 'no' then 'no'
else null
END;

ALTER TABLE product_information
DROP COLUMN promotions;

ALTER TABLE product_information
CHANGE COLUMN NewPromotions promotions ENUM('yes','no');

DESC product_information;

-- Sales data 
-- product id INT NOT NULL, sales date DATE, inventory_quantity INT, Product cost DECIMAL(10,2)
DESC sales_data; -- describe the table columns

ALTER TABLE sales_data
ADD COLUMN New_Sales_Date DATE;
UPDATE sales_data
SET New_Sales_Date = STR_TO_DATE(Sales_Date, '%d/%m/%Y');
ALTER TABLE sales_data
DROP COLUMN Sales_Date;
ALTER TABLE sales_data
CHANGE COLUMN New_Sales_Date Sales_Date DATE;

ALTER TABLE sales_data
MODIFY COLUMN product_cost DECIMAL(15,2);

-- identify missing values with 'IS NULL' function
-- for external factors
show columns from external_factors;
SELECT
SUM(case when Sales_Date IS NULL then 1 else 0 end) missing_sales_date,
SUM(case when GDP IS NULL then 1 else 0 end) missing_gdp,
SUM(case when inflation_rate IS NULL then 1 else 0 end) missing_inflation_rate,
SUM(case when seasonal_factor IS NULL then 1 else 0 end) missing_seasonal_factor
from external_factors;

-- for product information
DESC product_information;
SELECT
SUM(case when product_id IS NULL then 1 else 0 end) missing_product_id,
SUM(case when product_category IS NULL then 1 else 0 end) missing_product_category,
SUM(case when promotions IS NULL then 1 else 0 end) missing_promotions
from product_information;

-- for sales data
DESC sales_data;
SELECT
SUM(case when product_id IS NULL then 1 else 0 end) missing_product_id,
SUM(case when inventory_quantity IS NULL then 1 else 0 end) missing_inventory_quantity,
SUM(case when product_cost IS NULL then 1 else 0 end) missing_product_cost,
SUM(case when Sales_Date IS NULL then 1 else 0 end) missing_sales_date
from sales_data;

-- checking and removing duplicates using 'HAVING' and 'GROUP BY'
-- external factors
SELECT sales_date, count(*) sales_date_count
from external_factors
group by sales_date
having count(*)> 1;

SELECT COUNT(*) FROM (SELECT sales_date, count(*) sales_date_count
from external_factors
group by sales_date
having count(*)> 1) AS duplicates;
-- 352 duplicates found in external_factors

-- product information
SELECT product_id, count(*) product_id_count
from product_information
group by product_id
having count(*)> 1;

SELECT COUNT(*) FROM (SELECT product_id, count(*) product_id_count
from product_information
group by product_id
having count(*)> 1) AS duplicates;
-- 117 duplicates found in product information

-- Sales data
SELECT product_id, sales_date, count(*) product_id_count
from sales_data
group by 1,2
having count(*)> 1;
-- no duplicates found in sales data

-- Resolving Duplicates for external factors and product information
-- external factors
DELETE e1 from external_factors e1
INNER JOIN( 
SELECT sales_date, row_number() OVER (partition by sales_date order by sales_date) rn
from external_factors
) e2 ON e1.sales_date = e2.sales_date
WHERE e2.rn > 1;

-- product information
DELETE p1 from product_information p1
INNER JOIN( 
SELECT product_id, row_number() OVER (partition by product_id order by product_id) rn
from product_information
) p2
ON p1.product_id = p2.product_id
WHERE p2.rn > 1;

-- DATA INTEGRATION
-- Combine sales data and product informartion to form sales_product_data
CREATE VIEW sales_product_data AS
SELECT
s.product_id,
s.sales_date,
s.inventory_quantity,
s.product_cost,
p.product_category,
p.promotions
FROM sales_data s
JOIN product_information p
ON s.product_id = p.product_id;

-- combine sales_product_data and external_factors
CREATE VIEW main_Inventory_data AS 
SELECT
sp.product_id,
sp.sales_date,
sp.inventory_quantity,
sp.product_cost,
sp.product_category,
sp.promotions,
e.GDP,
e.inflation_rate,
e.seasonal_factor
FROM sales_product_data sp
JOIN external_factors e
ON sp.sales_date = e.sales_date;

-- Descriptive Analysis
-- Basic statistics:
-- Average sales (calculated as the product of 'Inventory Quantity' and 'Product Cost'
SELECT Product_id,
ROUND(AVG(inventory_quantity * product_cost),0) as avg_sales
from inventory_data
group by Product_id
Order by avg_sales DESC;

-- Median stock levels(i.e., 'Inventory Quantity').
SELECT product_id,
AVG(inventory_quantity) median_stock
FROM (
SELECT product_id,
		inventory_quantity,
row_number() OVER (partition by product_id order by inventory_quantity) AS row_num_asc,
row_number() OVER (partition by product_id order by inventory_quantity DESC) AS row_num_desc
	FROM main_inventory_data
) AS subquery
WHERE row_num_asc IN (row_num_desc, row_num_desc - 1, row_num_desc + 1)
group by product_id;
-- Product performance metrics (total sales per product)
SELECT product_id, 
ROUND(SUM(inventory_quantity * product_cost)) as total_sales
from inventory_data
group by product_id
order by total_sales DESC;

-- identify high demand products based on average sales
WITH highdemandproducts AS (
SELECT product_id, AVG(inventory_quantity) as avg_sales
from main_inventory_data
group by product_id
Having avg_sales > (
SELECT AVG(inventory_quantity)*0.95 From Sales_data
	)
)

-- Calculate stockout frequency for high demand products
SELECT d.product_id,
COUNT(*) as stockout_frequency
from main_inventory_data d
where d.product_id IN (SELECT product_id from highdemandproducts)
AND d.inventory_quantity=0
group by d.product_id;

-- Influence of external factors
''-- for GDP
SELECT product_id,
AVG(case when gdp > 0 then inventory_quantity else NULL end) as avg_sales_positive_gdp,
AVG(case when gdp <= 0 then inventory_quantity else NULL end) as avg_sales_nonpositive_gdp
from main_inventory_data
group by product_id
having avg_sales_positive_gdp IS NOT NULL;

''-- Inflation rate
SELECT product_id,
AVG(case when inflation_rate > 0 then inventory_quantity else NULL end) as avg_sales_positive_inflation,
AVG(case when inflation_rate <= 0 then inventory_quantity else NULL end) as avg_sales_negative_inflation
from main_inventory_data
group by product_id
having avg_sales_positive_inflation IS NOT NULL;

-- Optimizing Inventory
-- Determine the optimal reoder points for each product based on historical sales data and external factors
-- reorder point= Lead time demand + safety stock
-- Lead time demand = rolling avg sales * lead time
-- Safety stock = Z * root of Lead time(i.e lead time^-2) * standard deviation of demand
-- Z = 1.645
-- A consrant lead time of 7 days for all products.
-- aim for 95% service level
with inventorycalc as (
SELECT product_id,
AVG(rolling_avg_sales) as avg_rolling_sales,
AVG(rolling_variance) as avg_rolling_variance
FROM (
SELECT product_id,
AVG(daily_sales) OVER (PARTITION BY product_id ORDER BY sales_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_sales,
AVG(squared_diff) OVER (PARTITION BY product_id ORDER BY sales_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_variance
FROM (
SELECT product_id,
	sales_date, inventory_quantity * product_cost as daily_sales,
	(inventory_quantity * product_cost - AVG(inventory_quantity * product_cost) OVER (PARTITION BY product_id ORDER BY sales_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW))
    * (inventory_quantity * product_cost - AVG(inventory_quantity * product_cost) OVER (PARTITION BY product_id ORDER BY sales_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) as squared_diff
    FROM inventory_data
	) subquery
		) subquery2
        group by product_id
)
SELECT product_id,
avg_rolling_sales * 7 as lead_time_demand,
1.645 * (avg_rolling_variance * 7) as safety_stock,
(avg_rolling_sales * 7) + (1.645 * (avg_rolling_variance * 7)) as reorder_point
from inventorycalc;

-- create inventory_optimization table
CREATE TABLE inventory_optimization (
	product_id int,
reorder_point DOUBLE
);

-- create stored procedure to recalculate reorder point
DELIMITER //
CREATE procedure RecalculateReorderpoint(product_id INT)
BEGIN
	DECLARE avgRollingSales double;
    DECLARE avgRollingVariance DOUBLE;
    DECLARE leadTimeDemand DOUBLE;
    DECLARE reorderpoint DOUBLE;
    SELECT AVG(rolling_avg_sales) , AVG(rolling_variance) 
    INTO avgRollingSales, avgRollingVariance
FROM (
SELECT product_id,
AVG(daily_sales) OVER (PARTITION BY product_id ORDER BY sales_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_sales,
AVG(squared_diff) OVER (PARTITION BY product_id ORDER BY sales_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_variance
FROM (
SELECT product_id,
	sales_date, inventory_quantity * product_cost as daily_sales,
	(inventory_quantity * product_cost - AVG(inventory_quantity * product_cost) OVER (PARTITION BY product_id ORDER BY sales_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW))
    * (inventory_quantity * product_cost - AVG(inventory_quantity * product_cost) OVER (PARTITION BY product_id ORDER BY sales_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) as squared_diff
    FROM inventory_data
	) InnerDerived
		) OuterDerived;
	SET leadTimeDemand = avgRollingSales * 7;
    SET safetystock = 1.645 * SQRT(avgRollingVariance * 7);
    SET reorderpoint = leadDemand + safetyStock;


INSERT INTO inventory_optimization (product_id, reorder_point)
VALUES (productid, reorderpoint)
ON DUPLICATE KEY UPDATE Reorder_point = reorderpoint;
END //
DELIMITER ;

-- Step 3 make the inventory data a  permanent table
CREATE TABLE Inventory_Table as SELECT * FROM Inventory_data;
-- step 4 create a trigger
DELIMITER //
CREATE TRIGGER AfterInsertUnifiedTable
AFTER INSERT ON inventory_table
FOR EACH ROW
BEGIN
CALL RecalculateReorderpoint(NEW.product_id);
END //
DELIMITER ;

-- OVERSTOCK AND UNDERSTOCK
WITH rollingsales as (
SELECT product_id,
Sales_date,
AVG(Inventory_quantity*product_cost) OVER (partition by product_id order by sales_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rolling_avg_sales
FROM Inventory_table
),
-- Calc the number of days a product was out of stock
StockoutDays as (
SELECT product_id,
COUNT(*) as stockout_days
FROM inventory_table
where inventory_quantity = 0
group by Product_id
)
-- Join the above CTEs with main table to get results
SELECT i.product_id,
AVG(i.inventory_quantity * i.product_cost) as avg_inventory_value,
AVG(rs.rolling_avg_sales) as avg_rolling_sales,
COALESCE(sd.stockout_days, 0) as stockout_days
FROM inventory_table i
JOIN rollingsales rs ON i.product_id = rs.product_id AND i.sales_date = rs.sales_date
LEFT JOIN stockoutdays sd ON i.product_id = sd.product_id
group by i.product_id, sd.stockout_days;

-- MONITOR AND ADJUST
-- Inventory levels
	DELIMITER //
CREATE PROCEDURE MonitorInventoryLevels()
BEGIN
SELECT product_id, avg(inventory_quantity) as AvgInventory
FROM inventory_table
group by product_id
order by AvgInventory DESC;
END//
DELIMITER ;

-- monitor sales trend
DELIMITER //
CREATE PROCEDURE Monitorsalestrends()
BEGIN
SELECT product_id, sales_date,AVG(Inventory_quantity*product_cost) OVER (partition by product_id order by sales_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rolling_avg_sales
	FROM inventory_table
		order by product_id, sales_date;
END//
DELIMITER ;

-- monitor stockout frequencies
DELIMITER //
CREATE PROCEDURE Monitorstockouts()
BEGIN
SELECT product_id,
COUNT(*) as stockout_days
FROM inventory_table
where inventory_quantity = 0
group by Product_id
order by StockoutDays desc;
END//
DELIMITER ;

-- FEEDBACK LOOP
-- feedback loop establishment:
-- system monitoring: use SQL procedures to track system metrics, with deviations from expectations flagged for review.
-- Feedback Portal: create an online platform for stakeholders to easily submit feedback on inventory performance and challenges.
-- Review meetings: organize periodic sessions to discuss inventory system performance and gather insights.

-- Refinement based on Feedback:
-- Feedback analysis: Regularly compile and scrutinize feedback to identify recurring themes and pressing issues.
-- Action Implementation: Prioritize and act on the feedback to adjust reoerder points, safety stock levels, or overall processes.
-- Change Communication: Inform stakeholders about changes, underscoring the value of their feedback and ensuring transparency.


-- General Insights:
-- Inventory Discrepancies: The initial stages of the analysis revealed significant discrepancies in inventory levels, with instances of both overstocking and understocking.
-- These inconsistencies were contributing to capital inefficiencies and customer dissatisfaction.
-- Sales Trends and External Influences: The analysis indicated that sales trends were notably influenced by various external factors.
-- Recognizing these patterns provides an opportunity to forecast demand more accurately.
-- Suboptimal Inventory Levels: Through the inventory optimization analysis, it was evident that the existing inventory levels were not optimized for current sales trends.
-- Products was identified that had either close excess inventory.
 
-- Recommendations:
-- 1. Implement Dynamic Inventory Management: The company should transition from a static to a dynamic inventory management system,
-- adjusting inventory levels based on real-time sales trends, seasonality, and external factors.
-- 2. Optimize Reorder Points and Safety Stocks: Utilize the reorder points and safety stocks calculated during the analysis to minimize stockouts and reduce excess inventory.
-- Regularly review these metrics to ensure they align with current market conditions.
-- 3. Enhance Pricing Strategies: Conduct a thorough review of product pricing strategies, especially for products identified as unprofitable.
-- Consider factors such as competitor pricing, market demand, and product acquisition costs.
-- 4. Reduce Overstock: Identify products that are consistently overstocked and take steps to reduce their inventory levels.
-- This could include promotional sales, discounts, or even discontinuing products with low sales performance.
-- 5. Establish a Feedback Loop: Develop a systematic approach to collect and analyze feedback from various stakeholders.
-- Use this feedback for continuous improvement and alignment with business objectives.
-- 6. Regular Monitoring and Adjustments: Adopt a proactive approach to inventory management by regularly monitoring key metrics
-- and making necessary adjustments to inventory levels, order quantities, and safety stocks.
