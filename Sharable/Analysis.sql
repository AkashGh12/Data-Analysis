
-------------------------------Customer Demographics & Subscription Analysis----------------------------------

-- 1> How many customers are there in total?

select COUNT(distinct customerID) as total_customer from churn_data

-- 2> What is the gender distribution of customers?

select gender, count(distinct customerID) as [No. of customer] from churn_data
group by gender

-- 3> How many customers belong to each contract type ?

select contract, count(distinct customerID) as [No. of customer] from churn_data
group by contract

-- 4> What is the distribution of payment methods used by customers?

select paymentmethod, count(distinct customerID) as [No. of customer] from churn_data
group by paymentmethod

-- 5> What is the average tenure of customers?

select avg(tenure) as avg_tenure from churn_data


-- 6> What is the distribution of customers based on internet service type (DSL, Fiber optic, No Internet)?

select internetService, count(distinct customerID) as [No. of customer] from churn_data
group by internetService

 -------------------------------- Churn Analysis ----------------------------------------------

-- 7> What is the overall churn rate?

select 
cast(round(SUM(case when Churn = 1 then 1 else 0 end) *100.0 / 
(select COUNT(distinct customerID) from churn_data),2)as decimal (5,2)) as churn_rate
from churn_data

-- 8> How does churn vary by contract type?

select Contract, 
SUM(case when Churn = 1 then 1 else 0 end) as yes_churn,
SUM(case when Churn = 0 then 1 else 0 end) as no_churn,
COUNT(*) AS total_customers,
cast(ROUND(100.0 * SUM(CASE WHEN Churn = '1' THEN 1 ELSE 0 END) / COUNT(*), 2)as decimal(5,2)) AS churn_rate
from churn_data
group by Contract

-- 9> What is the churn rate for different payment methods?

select PaymentMethod, 
SUM(case when Churn = 1 then 1 else 0 end) as yes_churn,
SUM(case when Churn = 0 then 1 else 0 end) as no_churn,
COUNT(*) AS total_customers,
cast(ROUND(100.0 * SUM(CASE WHEN Churn = '1' THEN 1 ELSE 0 END) / COUNT(*), 2)as decimal(5,2)) AS churn_rate
from churn_data
group by PaymentMethod

-- 10> What is the churn rate for customers using different internet service types?

select InternetService, 
SUM(case when Churn = 1 then 1 else 0 end) as yes_churn,
SUM(case when Churn = 0 then 1 else 0 end) as no_churn,
COUNT(*) AS total_customers,
cast(ROUND(100.0 * SUM(CASE WHEN Churn = '1' THEN 1 ELSE 0 END) / COUNT(*), 2)as decimal(5,2)) AS churn_rate
from churn_data
group by InternetService

-- 11> Do customers with a higher monthly charge tend to churn more often?

select Churn, round(max(MonthlyCharges),2) as max_charge, round(MIN(MonthlyCharges),2) as min_charge,
round(avg(MonthlyCharges),2) as avg_charges
from churn_data
group by Churn
 
-------------------------------------Financial Analysis ---------------------------------------
-- 12> What is the total revenue generated from customers?

select round(SUM(TotalCharges),2) as total_revenue from churn_data

-- 13> What is the average monthly charge and total charges for customers?

select AVG(MonthlyCharges) as avg_monthlyCharges,
AVG(TotalCharges) as avg_TotalCharges
from churn_data

-- 14> What is the revenue distribution based on different contract types?

select Contract, round(SUM(TotalCharges),2) from churn_data
group by Contract

-- 15> What is the difference in revenue between churned and non-churned customers?

select case when churn = 0 then 'NO' 
			else 'YES' end as Churn,
totalRevenue from(
select Churn, round(SUM(TotalCharges),2) as totalRevenue from churn_data
group by Churn) as x

---------------------------  Service Usage & Customer Behavior ----------------------------------

-- 16> How many customers use additional services?

select case when OnlineSecurity = 0 then 'NO' 
			when OnlineSecurity = 1 then 'YES'
			else 'Dont Know' end as OnlineSecurity,
totalRevenue from(
select OnlineSecurity, round(SUM(TotalCharges),2) as totalRevenue from churn_data
group by OnlineSecurity) as x
order by totalRevenue

-- 17> What percentage of customers have a phone service vs. no phone service?

select 
cast(round(SUM(case when PhoneService = 1 then 1 else 0 end)*100.0 / (select COUNT(PhoneService) from churn_data),2)as decimal(5,2)) as phone_service,
cast(round(SUM(case when PhoneService = 0 then 1 else 0 end)*100.0 / (select COUNT(PhoneService) from churn_data),2) as decimal(5,2)) as Nophone_service
from churn_data

-- 18> What is the relationship between tenure and churn?
-- (Are long-term customers less likely to churn?)

select case when churn = 0 then 'NO' 
			else 'YES' end as Churn,
			avg_tenure from(
select Churn, AVG(tenure) as avg_tenure from churn_data
group by Churn) as x

-- long-term customers less likely to churn
 
-- 19> What is the average total charges for customers who have churned vs. those who have stayed?

select case when churn = 0 then 'NO' 
			else 'YES' end as Churn,
			avg_total_charges from(
select Churn, round(AVG(TotalCharges),2) as avg_total_charges from churn_data
group by Churn) as x

-- 20> Do customers with multiple lines have a higher churn rate?
SELECT category, COUNT(customerID) AS count 
FROM (
    SELECT *,
        CASE 
            WHEN MultipleLines = 0 AND Churn = 0 THEN 'Not Churned & No Multiple Lines'
            WHEN MultipleLines = 1 AND Churn = 0 THEN 'Not Churned & With Multiple Lines'
            WHEN MultipleLines = 0 AND Churn = 1 THEN 'Churned & No Multiple Lines'
            WHEN MultipleLines = 1 AND Churn = 1 THEN 'Churned & With Multiple Lines'
            ELSE 'Unknown'
        END AS category
    FROM churn_data
) AS x
where category != 'Unknown'
GROUP BY category


