CREATE MATERIALIZED VIEW zoohaib_gold.customer_sales AS
SELECT
  c.customer_id,
  c.customer_name,
  c.customer_email,
  c.customer_city,
  c.customer_state,
  p.product_name,
  p.product_category,
  SUM(s.total_amount) AS total_sales,
  COUNT(s.order_id) AS total_orders,
  AVG(s.total_amount) AS avg_order_value
FROM
  zoohaib_gold.customer_active c
JOIN
  zoohaib_silver.sales_pl_cleaned s
ON
  c.customer_id = s.customer_id
JOIN
  zoohaib_silver.products_pl_cleaned p
ON
  s.product_id = p.product_id
GROUP BY
ALL
ORDER BY
  total_sales DESC
LIMIT 10;