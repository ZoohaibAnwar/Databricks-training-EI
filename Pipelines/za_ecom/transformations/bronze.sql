CREATE STREAMING TABLE sales_pl AS
SELECT *, current_date() as ingestion FROM STREAM read_files(
  's3://jpmctraining/pipeline_input/sales',
  format => 'csv'
);

CREATE STREAMING TABLE products_pl AS
SELECT *, current_date() as ingestion FROM STREAM read_files(
  's3://jpmctraining/pipeline_input/products',
  format => 'csv'
);

CREATE STREAMING TABLE customers_pl AS
SELECT *, current_date() as ingestion FROM STREAM read_files(
  's3://jpmctraining/pipeline_input/customers',
  format => 'csv'
);