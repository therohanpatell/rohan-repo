-- Sample Data Inserts for DQ Validation Framework Testing

-- Insert Employees Data
INSERT INTO `project.dataset.employees` (employee_id, name, department, salary, hire_date, status, manager_id, email) VALUES
('E001', 'Rohan', 'Engineering', 75000.00, '2020-01-15', 'Active', 'M001', 'rohan@company.com'),
('E002', 'Rahul', 'Engineering', 80000.00, '2019-06-20', 'Active', 'M001', 'rahul@company.com'),
('E003', 'Priya', 'HR', 65000.00, '2021-03-10', 'Active', 'M002', 'priya@company.com'),
('E004', 'Rohan', 'Sales', 70000.00, '2020-08-05', 'Active', 'M003', 'rohan.s@company.com'),
('E005', 'Rahul', 'Marketing', 72000.00, '2021-11-12', 'Active', 'M004', 'rahul.m@company.com'),
('E006', 'Priya', 'Finance', 78000.00, '2019-02-28', 'Active', 'M005', 'priya.f@company.com'),
('E007', 'Rohan', 'Operations', 68000.00, '2022-01-20', 'Active', 'M006', 'rohan.o@company.com'),
('E008', 'Rahul', 'Engineering', 85000.00, '2018-09-15', 'Active', 'M001', 'rahul.e@company.com'),
('E009', 'Priya', 'HR', 67000.00, '2021-07-08', 'Inactive', 'M002', 'priya.h@company.com'),
('E010', 'Rohan', 'Sales', 73000.00, '2020-04-18', 'Active', 'M003', 'rohan.sales@company.com'),
('E011', 'Rahul', 'IT', 76000.00, '2019-12-01', 'Active', 'M007', 'rahul.it@company.com'),
('E012', 'Priya', 'Legal', 82000.00, '2020-10-22', 'Active', 'M008', 'priya.legal@company.com'),
(NULL, 'Unknown', 'Engineering', 60000.00, '2023-01-01', 'Active', 'M001', 'unknown@company.com'),
('E014', 'Rohan', 'Customer Service', 64000.00, '2021-05-15', 'Active', 'M009', 'rohan.cs@company.com'),
('E015', 'Rahul', 'Product', 88000.00, '2019-03-30', 'Active', 'M010', 'rahul.product@company.com');

-- Insert Orders Data
INSERT INTO `project.dataset.orders` (order_id, customer_id, order_date, status, amount, region, product_category) VALUES
('O001', 'C001', '2024-01-05', 'Completed', 1500.00, 'APAC', 'Electronics'),
('O002', 'C002', '2024-01-08', 'Completed', 2300.00, 'EMEA', 'Furniture'),
('O003', 'C003', '2024-01-10', 'Pending', 890.00, 'APAC', 'Clothing'),
('O004', 'C004', '2024-01-12', 'Completed', 4500.00, 'Americas', 'Electronics'),
('O005', 'C005', '2024-01-15', 'Shipped', 1200.00, 'APAC', 'Books'),
('O006', 'C001', '2024-01-18', 'Completed', 3400.00, 'EMEA', 'Electronics'),
('O007', 'C006', '2024-01-20', 'Cancelled', 560.00, 'Americas', 'Clothing'),
('O008', 'C007', '2024-01-22', 'Completed', 2100.00, 'APAC', 'Furniture'),
('O009', 'C008', '2024-01-25', 'Pending', 780.00, 'EMEA', 'Books'),
('O010', 'C002', '2024-01-28', 'Shipped', 1900.00, 'APAC', 'Electronics'),
('O011', 'C009', '2024-02-01', 'Completed', 3200.00, 'Americas', 'Furniture'),
('O012', 'C010', '2024-02-03', 'Completed', 1450.00, 'APAC', 'Clothing');

-- Insert Sales Data
INSERT INTO `project.dataset.sales` (sale_id, region, product, quantity, revenue, sale_date) VALUES
('S001', 'APAC', 'Laptop', 45, 67500.00, '2024-01-15'),
('S002', 'APAC', 'Mouse', 120, 3600.00, '2024-01-16'),
('S003', 'APAC', 'Keyboard', 80, 6400.00, '2024-01-17'),
('S004', 'EMEA', 'Laptop', 30, 45000.00, '2024-01-18'),
('S005', 'EMEA', 'Monitor', 25, 12500.00, '2024-01-19'),
('S006', 'Americas', 'Laptop', 55, 82500.00, '2024-01-20'),
('S007', 'Americas', 'Tablet', 40, 20000.00, '2024-01-21'),
('S008', 'APAC', 'Headphones', 90, 9000.00, '2024-01-22'),
('S009', 'EMEA', 'Webcam', 35, 7000.00, '2024-01-23'),
('S010', 'Americas', 'Mouse', 150, 4500.00, '2024-01-24');

-- Insert Customers Data
INSERT INTO `project.dataset.customers` (customer_id, customer_name, country, city, email, phone, registration_date, is_active) VALUES
('C001', 'Amit Kumar', 'India', 'Mumbai', 'amit@email.com', '+91-9876543210', '2023-01-10', TRUE),
('C002', 'Sarah Johnson', 'USA', 'New York', 'sarah@email.com', '+1-555-0101', '2023-02-15', TRUE),
('C003', 'Li Wei', 'China', 'Beijing', 'liwei@email.com', '+86-138-0000-0000', '2023-03-20', TRUE),
('C004', 'Emma Brown', 'UK', 'London', 'emma@email.com', '+44-20-7946-0958', '2023-04-05', TRUE),
('C005', 'Raj Patel', 'India', 'Delhi', 'raj@email.com', '+91-9876543211', '2023-05-12', TRUE),
('C006', 'John Smith', 'USA', 'Los Angeles', 'john@email.com', '+1-555-0102', '2023-06-18', FALSE),
('C007', 'Yuki Tanaka', 'Japan', 'Tokyo', 'yuki@email.com', '+81-3-1234-5678', '2023-07-22', TRUE),
('C008', 'Hans Mueller', 'Germany', 'Berlin', 'hans@email.com', '+49-30-12345678', '2023-08-30', TRUE),
('C009', 'Maria Garcia', 'Spain', 'Madrid', 'maria@email.com', '+34-91-123-4567', '2023-09-14', TRUE),
('C010', 'Priya Sharma', 'India', 'Bangalore', 'priya@email.com', '+91-9876543212', '2023-10-25', TRUE);

-- Insert Products Data
INSERT INTO `project.dataset.products` (product_id, product_name, category, price, stock_quantity, supplier_id, last_updated) VALUES
('P001', 'Laptop Pro 15', 'Electronics', 1500.00, 45, 'SUP001', '2024-01-15 10:30:00'),
('P002', 'Wireless Mouse', 'Electronics', 30.00, 200, 'SUP002', '2024-01-16 11:00:00'),
('P003', 'Mechanical Keyboard', 'Electronics', 80.00, 150, 'SUP002', '2024-01-17 09:45:00'),
('P004', '4K Monitor', 'Electronics', 500.00, 75, 'SUP001', '2024-01-18 14:20:00'),
('P005', 'Office Chair', 'Furniture', 250.00, 60, 'SUP003', '2024-01-19 08:15:00'),
('P006', 'Standing Desk', 'Furniture', 450.00, 30, 'SUP003', '2024-01-20 16:30:00'),
('P007', 'USB-C Hub', 'Electronics', 45.00, 180, 'SUP002', '2024-01-21 10:00:00'),
('P008', 'Webcam HD', 'Electronics', 200.00, 90, 'SUP001', '2024-01-22 13:45:00'),
('P009', 'Noise Cancelling Headphones', 'Electronics', 100.00, 120, 'SUP002', '2024-01-23 11:30:00'),
('P010', 'Tablet 10 inch', 'Electronics', 500.00, 85, 'SUP001', '2024-01-24 15:00:00');
