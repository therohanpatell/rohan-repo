-- BigQuery Table Schemas for DQ Validation Framework Testing

-- 1. Employees Table - For null checks, value checks, threshold checks
CREATE TABLE `project.dataset.employees` (
  employee_id STRING,
  name STRING,
  department STRING,
  salary FLOAT64,
  hire_date DATE,
  status STRING,
  manager_id STRING,
  email STRING
);

-- 2. Orders Table - For disallowed values, numeric conditions
CREATE TABLE `project.dataset.orders` (
  order_id STRING NOT NULL,
  customer_id STRING,
  order_date DATE,
  status STRING,
  amount FLOAT64,
  region STRING,
  product_category STRING
);

-- 3. Sales Table - For row match checks, aggregation validations
CREATE TABLE `project.dataset.sales` (
  sale_id STRING NOT NULL,
  region STRING,
  product STRING,
  quantity INT64,
  revenue FLOAT64,
  sale_date DATE
);

-- 4. Customers Table - For set match, pattern validations
CREATE TABLE `project.dataset.customers` (
  customer_id STRING NOT NULL,
  customer_name STRING,
  country STRING,
  city STRING,
  email STRING,
  phone STRING,
  registration_date DATE,
  is_active BOOLEAN
);

-- 5. Products Table - For range checks, duplicate checks
CREATE TABLE `project.dataset.products` (
  product_id STRING NOT NULL,
  product_name STRING,
  category STRING,
  price FLOAT64,
  stock_quantity INT64,
  supplier_id STRING,
  last_updated TIMESTAMP
);
