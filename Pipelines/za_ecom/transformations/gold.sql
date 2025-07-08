create materialized view if not exists zoohaib_gold.customer_active as
select customer_id, customer_name, customer_email, customer_city, customer_state from zoohaib_silver.customers_pl_cleaned where `__END_AT` is null ;



create materialized view zoohaib_gold.top_customers as
select
  s.customer_id,
  c.customer_name,
  c.customer_email,
  c.customer_city,
  sum(s.total_amount) as total_sales
from zoohaib_silver.sales_pl_cleaned s
join zoohaib_gold.customer_active c
  on s.customer_id = c.customer_id
group by all
order by total_sales desc
limit 3