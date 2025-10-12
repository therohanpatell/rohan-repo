# DQ Validation Framework - Sample Data Summary

## Overview

I've created comprehensive sample data and configuration files to help you test all validation scenarios in the DQ framework.

## What's Included

### 1. BigQuery Table Schemas (`sample_data/bq_schemas.sql`)
5 tables designed to test all validation scenarios:
- **employees** (15 rows) - Null checks, value checks, threshold checks
- **orders** (12 rows) - Disallowed values, numeric conditions
- **sales** (10 rows) - Row match checks, aggregation validations
- **customers** (10 rows) - Set match, pattern validations
- **products** (10 rows) - Range checks, duplicate checks

### 2. Sample Data (`sample_data/insert_data.sql`)
57 total rows of realistic test data with:
- 1 intentional NULL value (for testing null checks)
- Multiple distinct values for set matching
- Aggregatable data for row matching
- Various numeric ranges for threshold testing

### 3. Comprehensive DQ Configuration (`sample_dq_config_comprehensive.json`)
20 validation checks covering ALL scenarios:

| Validation Type | Count | Check IDs |
|----------------|-------|-----------|
| Null Checks | 2 | CHK_001, CHK_020 |
| Set Match | 4 | CHK_002, CHK_010, CHK_012, CHK_013 |
| Not In Result | 2 | CHK_003, CHK_011 |
| Numeric Conditions | 10 | CHK_004, CHK_006-009, CHK_014-017, CHK_019 |
| Row Match | 2 | CHK_005, CHK_018 |

### 4. Setup Scripts
- **Linux/Mac**: `sample_data/setup_bq_tables.sh`
- **Windows**: `sample_data/setup_bq_tables.ps1`

Both scripts automatically:
- Create BigQuery dataset (if needed)
- Create all tables
- Insert sample data
- Verify data insertion
- Update configuration file with your project/dataset names

### 5. Documentation
- **README.md** - Step-by-step setup guide
- **VALIDATION_SCENARIOS.md** - Complete reference for all validation types
- **SAMPLE_DATA_SUMMARY.md** - This file

## Quick Start

### Option 1: Automated Setup (Recommended)

**Linux/Mac:**
```bash
cd sample_data
chmod +x setup_bq_tables.sh
./setup_bq_tables.sh YOUR_PROJECT_ID YOUR_DATASET_NAME
```

**Windows PowerShell:**
```powershell
cd sample_data
.\setup_bq_tables.ps1 -ProjectId "YOUR_PROJECT_ID" -DatasetName "YOUR_DATASET_NAME"
```

### Option 2: Manual Setup

1. **Create tables:**
   ```bash
   bq query --use_legacy_sql=false < sample_data/bq_schemas.sql
   ```

2. **Insert data:**
   ```bash
   bq query --use_legacy_sql=false < sample_data/insert_data.sql
   ```

3. **Update config file:**
   Replace `project.dataset` with your actual project and dataset in `sample_dq_config_comprehensive.json`

### Run the Framework

```bash
python main.py --config sample_dq_config_comprehensive.json
```

## Expected Results

When you run the framework with the sample data:

✅ **19 checks ACTIVE** (active: true)  
❌ **1 check INACTIVE** (active: false) - CHK_020  
✅ **18 checks PASS**  
❌ **1 check FAILS** - CHK_001 (intentional null value)

## All Validation Scenarios Covered

### 1. Numeric Conditions (10 checks)
- ✅ Exact match (=) - CHK_008
- ✅ Not equal (!=) - CHK_009
- ✅ Greater than (>) - CHK_007
- ✅ Greater than or equal (>=) - CHK_004, CHK_014, CHK_017
- ✅ Less than (<) - CHK_016
- ✅ Less than or equal (<=) - CHK_006
- ❌ Null check (=0) - CHK_001 (FAILS), CHK_020 (INACTIVE)
- ✅ Duplicate check (=0) - CHK_019

### 2. Set Match (4 checks)
- ✅ Employee names - CHK_002
- ✅ Customer countries - CHK_010
- ✅ Product categories - CHK_012
- ✅ Order regions - CHK_013

### 3. Not In Result (2 checks)
- ✅ No invalid order status - CHK_003
- ✅ No failed orders - CHK_011

### 4. Row Match (2 checks)
- ✅ Region sales counts - CHK_005
- ✅ Country customer counts - CHK_018

## Sample Data Details

### Employees Table
```
Rows: 15
Columns: employee_id, name, department, salary, hire_date, status, manager_id, email
Key Features:
- 1 NULL employee_id (E013 row)
- 3 unique names: Rohan (5), Rahul (5), Priya (4), Unknown (1)
- 12 unique departments
- Salary range: 60,000 - 88,000
- 14 Active, 1 Inactive
```

### Orders Table
```
Rows: 12
Columns: order_id, customer_id, order_date, status, amount, region, product_category
Key Features:
- Status: Completed (7), Pending (2), Shipped (2), Cancelled (1)
- Regions: APAC (6), EMEA (3), Americas (3)
- Amount range: 560 - 4,500
- No INVALID or ERROR status
```

### Sales Table
```
Rows: 10
Columns: sale_id, region, product, quantity, revenue, sale_date
Key Features:
- Regions: APAC (4), EMEA (3), Americas (3)
- Total revenue: ~258,000
- Products: Laptop, Mouse, Keyboard, Monitor, Tablet, Headphones, Webcam
```

### Customers Table
```
Rows: 10
Columns: customer_id, customer_name, country, city, email, phone, registration_date, is_active
Key Features:
- 7 countries: India (3), USA (2), China (1), UK (1), Japan (1), Germany (1), Spain (1)
- 9 active, 1 inactive
- No NULL customer_ids
```

### Products Table
```
Rows: 10
Columns: product_id, product_name, category, price, stock_quantity, supplier_id, last_updated
Key Features:
- 2 categories: Electronics (8), Furniture (2)
- Stock range: 30 - 200
- Price range: 30 - 1,500
- No duplicate product_ids
```

## Validation Check Details

| Check ID | Type | Expected | Actual | Result |
|----------|------|----------|--------|--------|
| CHK_001 | Null Check | 0 | 1 | ❌ FAIL |
| CHK_002 | Set Match | [Rohan, Rahul, Priya] | [Rohan, Rahul, Priya] | ✅ PASS |
| CHK_003 | Not In Result | No [INVALID, ERROR] | No invalid values | ✅ PASS |
| CHK_004 | Threshold >= | 10 | 12 | ✅ PASS |
| CHK_005 | Row Match | 3 regions | Matches | ✅ PASS |
| CHK_006 | Threshold <= | 100000 | 88000 | ✅ PASS |
| CHK_007 | Threshold > | 50000 | 60000 | ✅ PASS |
| CHK_008 | Exact = | 15 | 15 | ✅ PASS |
| CHK_009 | Not Equal != | 0 | 9 | ✅ PASS |
| CHK_010 | Set Match | 7 countries | Matches | ✅ PASS |
| CHK_011 | Not In Result | No [Failed, Rejected] | No invalid | ✅ PASS |
| CHK_012 | Set Match | 2 categories | Matches | ✅ PASS |
| CHK_013 | Set Match | 3 regions | Matches | ✅ PASS |
| CHK_014 | Threshold >= | 10 | 12 | ✅ PASS |
| CHK_015 | Threshold > | 1000 | ~1900 | ✅ PASS |
| CHK_016 | Threshold < | 300000 | ~258000 | ✅ PASS |
| CHK_017 | Threshold >= | 30 | 30 | ✅ PASS |
| CHK_018 | Row Match | 2 countries | Matches | ✅ PASS |
| CHK_019 | Duplicate Check | 0 | 0 | ✅ PASS |
| CHK_020 | Null Check | 0 | N/A | ⏸️ INACTIVE |

## Customization

### Add More Checks
Edit `sample_dq_config_comprehensive.json` to add your own validation rules.

### Modify Sample Data
Edit `sample_data/insert_data.sql` to change test scenarios.

### Test Edge Cases
- Add more NULL values
- Add duplicate records
- Add invalid status values
- Modify counts to test thresholds

## Troubleshooting

### Authentication
```bash
gcloud auth application-default login
```

### Permissions
Ensure you have:
- BigQuery Data Editor
- BigQuery Job User

### Common Issues
1. **"Table not found"** - Verify project and dataset names
2. **"Permission denied"** - Check IAM roles
3. **"Query failed"** - Check SQL syntax in config file

## Next Steps

1. ✅ Run the framework with sample data
2. ✅ Review validation results
3. ✅ Understand each validation type
4. ✅ Modify checks for your use case
5. ✅ Add your own tables and validations
6. ✅ Integrate with CI/CD pipeline
7. ✅ Set up monitoring and alerting

## Support

For detailed information on each validation type, see:
- `sample_data/VALIDATION_SCENARIOS.md` - Complete validation reference
- `sample_data/README.md` - Setup instructions
- Design document - Framework architecture

## Files Created

```
.
├── sample_dq_config_comprehensive.json (20 validation checks)
├── SAMPLE_DATA_SUMMARY.md (this file)
└── sample_data/
    ├── README.md (setup guide)
    ├── VALIDATION_SCENARIOS.md (validation reference)
    ├── bq_schemas.sql (table schemas)
    ├── insert_data.sql (sample data)
    ├── setup_bq_tables.sh (Linux/Mac setup script)
    └── setup_bq_tables.ps1 (Windows setup script)
```

Happy validating! 🚀
