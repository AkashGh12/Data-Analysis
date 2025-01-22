create database pizza_sales

select * from pizza_sales

-- KPI
-- 1. Total Revenue:

select SUM(total_price) as Total_Revenue from pizza_sales

-- 2. Average Order Value:

select (SUM(total_price)/COUNT(distinct order_id)) as average_value from pizza_sales

-- 3. Total Pizzas Sold:

select SUM(quantity) as pizza_sold from pizza_sales

-- 4. Total Orders:

select count(distinct order_id) as number_of_orders from pizza_sales

-- 5. Average Pizzas Per Order

select CAST(CAST(SUM(quantity) AS DECIMAL(10,2)) / 
CAST(COUNT(DISTINCT order_id) AS DECIMAL(10,2)) AS DECIMAL(10,2))
 as pizza_sold from pizza_sales