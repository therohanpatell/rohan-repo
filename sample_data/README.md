# DQ Validation Framework - Sample Data Setup Guide

This guide will help you set up sample data in BigQuery and run the DQ validation framework.

## Overview

The sample data includes 5 tables covering various validation scenarios:
- **employees** - For null checks, value checks, threshold checks
- **orders** - For disallowed values, numeric conditions
- **sales** - For row match checks, aggregation validations
- **customers** - For set match, pattern validations
- **products** - For range checks, duplicate checks

## Step 1: Create BigQuery Tables

Replace `project.dataset` with your actual BigQuery project and dataset names.

```bash
# Run the schema creation script
bq query --use_legacy_sql=false < bq_schemas.sql
```

Or manually create tables using the BigQuery Console:
1. Open BigQuery Console
2. Select your dataset
3. Copy and execute each CREATE TABLE statement from `bq_schemas.sql`

## Step 2: Insert Sample Data

```bash
# Run the data insert script
bq query --use_legacy_sql=false < insert_data.sql
```

Or manually insert data using the BigQuery Console:
1. Copy and execute the INSERT statements from `insert_data.sql`

## Step 3: Verify Data

Run these queries to verify data was inserted correctly:

```sql
-- Check row counts
SELECT 'employees' as table_name, COUNT(*) as row_count FROM `project.dataset.employees`
UNION ALL
SELECT 'orders', COUNT(*) FROM `project.dataset.orders`
UNION ALL
SELECT 'sales', COUNT(*) FROM `project.dataset.sales`
UNION ALL
SELECT 'customers', COUNT(*) FROM `project.dataset.customers`
UNION ALL
SELECT 'products', COUNT(*) FROM `project.dataset.products`;
```

Expected results:
- employees: 15 rows
- orders: 12 rows
- sales: 10 rows
- customers: 10 rows
- products: 10 rows

## Step 4: Update DQ Configuration

Update the `sample_dq_config_comprehensive.json` file:

1. Replace all instances of `project.dataset` with your actual BigQuery project and dataset
2. You can use this command:

```bash
# Linux/Mac
sed -i 's/project\.dataset/YOUR_PROJECT.YOUR_DATASET/g' ../sample_dq_config_comprehensive.json

# Windows PowerShell
(Get-Content ../sample_dq_config_comprehensive.json) -replace 'project\.dataset', 'YOUR_PROJECT.YOUR_DATASET' | Set-Content ../sample_dq_config_comprehensive.json
```

## Step 5: Run DQ Validation Framework

```bash
# Navigate to your project root
cd ..

# Run the framework with the comprehensive config
python main.py --config sample_dq_config_comprehensive.json
```

## Validation Scenarios Covered

The comprehensive configuration includes 20 checks covering all scenarios:

### 1. Null Checks (numeric_condition with "0")
- **CHK_001**: Employee ID null check (Expected to FAIL - 1 null exists)
- **CHK_020**: Customer ID null check (INACTIVE example)

### 2. Set Match (allowed values)
- **CHK_002**: Employee names must be Rohan, Rahul, or Priya
- **CHK_010**: Customer countries validation
- **CHK_012**: Product categories (Electronics, Furniture)
- **CHK_013**: Order regions (APAC, Americas, EMEA)

### 3. Not In Result (disallowed values)
- **CHK_003**: No INVALID or ERROR status in orders
- **CHK_011**: No Failed or Rejected orders

### 4. Numeric Conditions (thresholds)
- **CHK_004**: At least 10 departments (>=10)
- **CHK_006**: Max salary <= 100000
- **CHK_007**: Min salary > 50000
- **CHK_008**: Exact employee count = 15
- **CHK_009**: Active customers != 0
- **CHK_014**: Total orders >= 10
- **CHK_015**: Average order amount > 1000
- **CHK_016**: Total revenue < 300000
- **CHK_017**: Min stock quantity >= 30
- **CHK_019**: No duplicate product IDs = 0

### 5. Row Match (array of objects)
- **CHK_005**: Region-wise sales counts
- **CHK_018**: Customer counts by country (India, USA)

## Expected Results

When you run the framework:
- **19 checks** should be ACTIVE (active: true)
- **1 check** should be INACTIVE (active: false) - CHK_020
- **18 checks** should PASS
- **1 check** should FAIL - CHK_001 (null employee_id check)

## Sample Data Characteristics

### Employees Table (15 rows)
- 1 NULL employee_id (intentional for testing)
- 3 unique names: Rohan, Rahul, Priya
- 12 unique departments
- Salary range: 60,000 - 88,000
- Mix of Active/Inactive status

### Orders Table (12 rows)
- Status values: Completed, Pending, Shipped, Cancelled
- Regions: APAC, EMEA, Americas
- Amount range: 560 - 4500
- No INVALID or ERROR status (for testing not_in_result)

### Sales Table (10 rows)
- 3 regions: APAC (4), EMEA (3), Americas (3)
- Various products with quantities and revenue
- Total revenue: ~258,000

### Customers Table (10 rows)
- 7 countries: India, USA, China, UK, Japan, Germany, Spain
- 9 active, 1 inactive customer
- No NULL customer_ids

### Products Table (10 rows)
- 2 categories: Electronics, Furniture
- Stock quantities: 30-200
- Price range: 30 - 1500
- No duplicate product_ids

## Troubleshooting

### Authentication Issues
```bash
# Authenticate with Google Cloud
gcloud auth application-default login
```

### Permission Issues
Ensure your service account or user has:
- BigQuery Data Editor role
- BigQuery Job User role

### Query Errors
- Verify project and dataset names are correct
- Check that tables were created successfully
- Ensure data was inserted properly

## Next Steps

1. Review the validation results
2. Modify checks to test different scenarios
3. Add your own custom validation rules
4. Integrate with your CI/CD pipeline
5. Set up alerting for failed checks
