select * from pizza_sales

-- B. Daily Trend for Total Orders

select DATENAME(DW,order_date) as Weekday, count(distinct(order_id)) as [Number of orders] from pizza_sales
group by DATENAME(DW,order_date)

-- C. Monthly Trend for Orders

select DATENAME(MONTH,order_date) as Months, count(distinct(order_id)) as [Number of orders] from pizza_sales
group by DATENAME(MONTH,order_date)

-- D. % of Sales by Pizza Category

select distinct(pizza_category), cast(SUM(total_price) as decimal(10,2)) as Total_Revenue,
CAST(SUM(total_price) * 100 / (SELECT SUM(total_price) from pizza_sales) AS DECIMAL(10,2)) AS PCT
from pizza_sales 
group by pizza_category
order by pizza_category

-- E. % of Sales by Pizza Size

select distinct(pizza_size), cast(SUM(total_price) as decimal(10,2)) as Total_Revenue,
CAST(SUM(total_price) * 100 / (SELECT SUM(total_price) from pizza_sales) AS DECIMAL(10,2)) AS PCT
from pizza_sales 
group by pizza_size
order by pizza_size

-- F. Total Pizzas Sold by Pizza Category

select distinct(pizza_category), COUNT(quantity) as Total_quantity
from pizza_sales 
group by pizza_category
order by pizza_category

-- G. Top 5 Pizzas by Revenue

select top 5 pizza_name, cast(SUM(total_price) as decimal(10,2)) as Revenue from pizza_sales 
group by pizza_name
order by cast(SUM(total_price) as decimal(10,2)) desc

-- H. Bottom 5 Pizzas by Revenue

select top 5 pizza_name, cast(SUM(total_price) as decimal(10,2)) as Revenue from pizza_sales 
group by pizza_name
order by cast(SUM(total_price) as decimal(10,2)) asc

-- I. Top 5 Pizzas by Quantity

select top 5 pizza_name, COUNT(quantity) as Quantity from pizza_sales 
group by pizza_name
order by COUNT(quantity) desc

-- J. Bottom 5 Pizzas by Quantity

select top 5 pizza_name, COUNT(quantity) as Quantity from pizza_sales 
group by pizza_name
order by COUNT(quantity) asc

-- K. Top 5 Pizzas by Total Orders

select top 5 pizza_name, COUNT(distinct order_id) as orders from pizza_sales 
group by pizza_name
order by COUNT(distinct order_id) desc

-- L. Bottom 5 Pizzas by Total Orders

select top 5 pizza_name, COUNT(distinct order_id) as orders from pizza_sales 
group by pizza_name
order by COUNT(distinct order_id) asc
