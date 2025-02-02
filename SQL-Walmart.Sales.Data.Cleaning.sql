# Create database walmart
CREATE DATABASE Walmart_data_cleaning;

# Use database
 USE walmart_data_cleaning;
# Create table sales

CREATE TABLE sales( invoice_id VARCHAR(20) PRIMARY KEY,
                    branch VARCHAR(20),
                    store_location VARCHAR(20),
					customer_type VARCHAR(50),
                    gender VARCHAR(20),
                    product_category VARCHAR(50),
                    unit_price DECIMAL(10,2),
                    quantity INT(10),
                    vat DECIMAL(10,2),
                    total_sale DECIMAL(10,2),	     
                    sale_date DATE,	
                    sale_time TIME,	
                    payment VARCHAR(30),	
                    cogs DECIMAL(10,2),	
                    gross_margin_pct DECIMAL(10,2),
                    gross_income DECIMAL(10,2),	
                    rating DECIMAL(10,2));

# Check Variables 
SHOW GLOBAL VARIABLES LIKE "LOCAL_INFILE";

# If OF then Convert into ON 
SET GLOBAL LOCAL_INFILE =TRUE; 

# Load Data into SQL

LOAD DATA LOCAL INFILE "C:\\Users\\Aniket\\Downloads\\Walmart Sales Data.csv (2).csv"
INTO TABLE sales
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


# Reterive data from sales
SELECT * FROM sales;

# Rename  the column name  
ALTER TABLE sales  RENAME COLUMN grsalesoss_margin_pct TO gross_margin_pct;

# Change Datatype and column name
ALTER TABLE sales CHANGE COLUMN payment payment_mode VARCHAR(50);

-------------------------- Data Cleaning Steps ------------------------------
------------------------- Step 1: Identify Missing Values -------------------

# COUNT NULL values 

SELECT COUNT(*) AS missing_unit_price
FROM sales 
WHERE unit_price is NULL;

SELECT COUNT(*) AS product_category
FROM sales
WHERE product_category IS NULL;

SELECT COUNT(*) AS missing_total_sale
FROM sales
WHERE total_sale IS NULL;

SELECT COUNT(*) AS missing_payment_mode
FROM sales
WHERE payment_mode IS  NULL;

--------------------------- SUM CASE WHEN Function -----------------------
SELECT 
SUM(CASE WHEN unit_price IS NULL THEN 1 ELSE 0 END) AS missing_unit_price,
SUM(CASE WHEN product_category IS NULL THEN 1 ELSE 0 END) AS missing_product_category,
SUM(CASE WHEN product_category IS NULL THEN 1 ELSE 0 END) AS missing_total_sale,
SUM(CASE WHEN payment_mode IS NULL  THEN 1 ELSE 0 END) AS payment_mode
FROM sales;

------------------------- Step 2.Fixed NULL Values----------------------------
# 1. Unit Price
# ON UPDATE Safety Mode
 SET SQL_SAFE_UPDATES = 0;

SET @avg_unit_price = (SELECT AVG(unit_price) FROM sales);

UPDATE sales
SET unit_price =  @avg_unit_price
WHERE unit_price IS NULL;

SELECT COUNT(*) as missing_unit_price
FROM sales
WHERE unit_price IS NULL;

# 2. Product_Category
#----------Using Mode Method Beacause of categorial data ------

SELECT product_category AS  mode_product_category
FROM (
    SELECT product_category, COUNT(*) AS frequency
    FROM sales
    GROUP BY product_category
    ORDER BY frequency DESC
    LIMIT 1
) AS subquery;
 
UPDATE sales
SET product_category="Fashion accessories"
WHERE product_category IS NULL;

SELECT COUNT(*) as missing_product_category
FROM sales
WHERE unit_price IS NULL;

# 3.total_sale
SET @avg_total_sale = (SELECT AVG(total_sale) FROM sales);

UPDATE sales
SET total_sale= @avg_total_sale
WHERE total_sale IS NULL;

# 4. payment_mode
SELECT payment_mode AS mode_of_payment_mode
FROM(
SELECT payment_mode, COUNT(*) as frequency
FROM sales
GROUP BY payment_mode
ORDER BY frequency DESC
LIMIT 1) AS Frequency;

UPDATE sales
SET payment_mode= "Ewallet"
WHERE payment_mode IS NULL;  

SELECT COUNT(*)
FROM sales
WHERE payment_mode is null;

--------------- Step 3: Remove Duplicates--------------------
# Identify Duplicates
SELECT invoice_id, COUNT(*) as Duplicate_Count
FROM sales
GROUP BY invoice_id
Having COUNT(*) > 1;

DELETE FROM sales 
WHERE invoice_id NOT IN (
    SELECT invoice_id FROM (
        SELECT MIN(invoice_id) AS invoice_id 
        FROM sales  
        GROUP BY product_category, unit_price, total_sale, payment_mode
    ) AS derived_table
);

 # -----------------------Step 4: Fix Incorrect Data Formats-------------------
 # --1. Convert Sale_Date to YYYY-MM-DD Format
UPDATE sales
SET sale_date = STR_To_DATE(sale_date,'%Y-%m-%d'); 

UPDATE sales 
SET sale_time = TIME_FORMAT(sale_time, '%H-%i-%s');

#-----------------------Step 5: Handle Outliers in Total_Sale-------------------

# --1.Identify Outliers using IQR
WITH quartiles AS (
    SELECT 
        Total_Sale,
        NTILE(4) OVER (ORDER BY Total_Sale) AS quartile
    FROM sales
),
iqr_values AS (
    SELECT 
        MIN(CASE WHEN quartile = 1 THEN Total_Sale END) AS Q1,
        MAX(CASE WHEN quartile = 3 THEN Total_Sale END) AS Q3
    FROM quartiles
)
DELETE FROM sales
WHERE Total_Sale < (SELECT Q1 - 1.5 * (Q3 - Q1) FROM iqr_values)
   OR Total_Sale > (SELECT Q3 + 1.5 * (Q3 - Q1) FROM iqr_values);

# -------------------------Step 6: Feature Engineering-------------------------------
# Categorize Products as ‘Good’ or ‘Bad’ Based on Sales Performance

# Step 1: Declare a variable to store the average total_sale

SET @avg_total_sale = (SELECT AVG(total_sale) FROM sales);
 
# --Step 2: Use the variable in the UPDATE statement 
 UPDATE sales
 SET product_performance = (
 CASE 
WHEN  total_sale >= @avg_total_sale  THEN 'GOOD'
ELSE 'BAD'
END);

# Categorize Sales into Time Slots

# CREATE Temporary Table
SELECT sale_time,(
CASE 
WHEN sale_time BETWEEN '00:00:00' AND '12:00:00' THEN 'Morning'
WHEN sale_time BETWEEN '12:01:00' AND '06:00:00' THEN 'Afternoon'
ELSE 'Evening'
END) AS time_of_day
FROM sales;

# CREATE a new column 
ALTER TABLE sales ADD COLUMN time_of_day VARCHAR(10);

# Update table values 
UPDATE sales
SET time_of_day =(
CASE 
WHEN sale_time BETWEEN '00:00:00' AND '12:00:00' THEN 'Morning'
WHEN sale_time BETWEEN '12:01:00' AND '06:00:00' THEN 'Afternoon'
ELSE 'Evening'
END);

# DAYName
# CREATE Temporary Table 
SELECT sale_date,
DAYNAME(sale_date) as day_name
FROM sales;

# CREATE PERMENT TABLE 
ALTER TABLE sales ADD COLUMN day_name VARCHAR(50);
UPDATE sales
SET day_name = DAYNAME(sale_date);

# MONTHNAME
# CRATE TABLE Month_name
SELECT sale_date,
MONTHNAME(sale_date) as month_name
from sales;

# CREATE TABLE MONTH_NAME 
ALTER TABLE sales ADD COLUMN month_name VARCHAR(20);

# UPDATE DATA 
Update sales
SET month_name = MONTHNAME(sale_date);

#------------------------Step 7: Save Cleaned Data------------------------
SELECT * FROM sales
INTO OUTFILE 'C:/Cleaned_Walmart_Sales_Data.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';

#  Final Summary
# ✔ Checked raw data
# ✔ Handled missing values
# ✔ Removed duplicates
# ✔ Fixed incorrect formats
# ✔ Identified and removed outliers
# ✔ Performed feature engineering
# ✔ Saved cleaned data for further analysis




