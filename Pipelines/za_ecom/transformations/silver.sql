CREATE STREAMING TABLE zoohaib_silver.sales_pl_cleaned
(CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW)
AS
SELECT distinct * from stream sales_pl;

CREATE OR REFRESH STREAMING TABLE zoohaib_silver.products_pl_cleaned;

CREATE FLOW product_flow AS AUTO CDC INTO
  zoohaib_silver.products_pl_cleaned
FROM
  stream(products_pl)
KEYS
  (product_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  seqNum
COLUMNS * EXCEPT
  (operation, seqNum, _rescued_data,ingestion )
STORED AS
  SCD TYPE 1;


CREATE OR REFRESH STREAMING TABLE zoohaib_silver.customers_pl_cleaned;

CREATE FLOW customer_flow AS AUTO CDC INTO
  zoohaib_silver.customers_pl_cleaned
FROM
  stream(customers_pl)
KEYS
  (customer_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  sequenceNum
COLUMNS * EXCEPT
  (operation, sequenceNum, _rescued_data,ingestion )
STORED AS
  SCD TYPE 2;