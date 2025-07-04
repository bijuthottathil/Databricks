CREATE OR REFRESH STREAMING TABLE customers_cleaned
AS
AUTO CDC INTO customers_cleaned
FROM STREAM customers_raw
OPTIONS (
  key = "customer_id",
  sequence_by = "_commit_version",
  apply_as = "_change_type"
);