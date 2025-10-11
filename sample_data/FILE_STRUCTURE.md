# Sample Data File Structure

## Complete File Tree

```
project-root/
│
├── sample_dq_config_comprehensive.json    # 20 validation checks (MAIN CONFIG)
├── SAMPLE_DATA_SUMMARY.md                 # Complete overview and guide
│
└── sample_data/                           # All sample data files
    │
    ├── README.md                          # Step-by-step setup instructions
    ├── QUICK_REFERENCE.md                 # Quick reference card
    ├── VALIDATION_SCENARIOS.md            # Complete validation reference
    ├── FILE_STRUCTURE.md                  # This file
    │
    ├── bq_schemas.sql                     # BigQuery table schemas (5 tables)
    ├── insert_data.sql                    # Sample data inserts (57 rows)
    │
    ├── setup_bq_tables.sh                 # Automated setup for Linux/Mac
    └── setup_bq_tables.ps1                # Automated setup for Windows
```

## File Descriptions

### Configuration Files

#### `sample_dq_config_comprehensive.json` ⭐ MAIN FILE
- **Purpose**: Complete DQ validation configuration
- **Contains**: 20 validation checks covering all scenarios
- **Use**: Run framework with this config
- **Command**: `python main.py --config sample_dq_config_comprehensive.json`

### Documentation Files

#### `SAMPLE_DATA_SUMMARY.md` 📋 START HERE
- **Purpose**: Complete overview of sample data setup
- **Contains**: 
  - What's included
  - Quick start guide
  - Expected results
  - All validation scenarios
  - Sample data details
- **Audience**: First-time users

#### `sample_data/README.md` 📖 SETUP GUIDE
- **Purpose**: Detailed setup instructions
- **Contains**:
  - Step-by-step BigQuery setup
  - Data verification queries
  - Configuration update instructions
  - Troubleshooting guide
- **Audience**: Users setting up manually

#### `sample_data/QUICK_REFERENCE.md` ⚡ CHEAT SHEET
- **Purpose**: Quick reference for common tasks
- **Contains**:
  - Setup commands
  - Validation type examples
  - SQL patterns
  - Configuration templates
- **Audience**: Experienced users

#### `sample_data/VALIDATION_SCENARIOS.md` 📚 COMPLETE REFERENCE
- **Purpose**: Comprehensive validation documentation
- **Contains**:
  - All validation types explained
  - Examples for each scenario
  - Best practices
  - Advanced scenarios
  - Testing guide
- **Audience**: Developers extending the framework

#### `sample_data/FILE_STRUCTURE.md` 🗂️ THIS FILE
- **Purpose**: Navigate the file structure
- **Contains**: File tree and descriptions

### SQL Files

#### `sample_data/bq_schemas.sql` 🗄️ TABLE DEFINITIONS
- **Purpose**: Create BigQuery tables
- **Contains**: DDL for 5 tables
  - employees (8 columns)
  - orders (7 columns)
  - sales (6 columns)
  - customers (8 columns)
  - products (7 columns)
- **Usage**: `bq query --use_legacy_sql=false < bq_schemas.sql`

#### `sample_data/insert_data.sql` 📊 SAMPLE DATA
- **Purpose**: Insert test data into tables
- **Contains**: 57 rows across 5 tables
  - employees: 15 rows (1 with NULL employee_id)
  - orders: 12 rows
  - sales: 10 rows
  - customers: 10 rows
  - products: 10 rows
- **Usage**: `bq query --use_legacy_sql=false < insert_data.sql`

### Setup Scripts

#### `sample_data/setup_bq_tables.sh` 🐧 LINUX/MAC SETUP
- **Purpose**: Automated setup for Unix systems
- **Features**:
  - Creates dataset if needed
  - Creates all tables
  - Inserts sample data
  - Verifies data
  - Updates config file
  - Color-coded output
- **Usage**: `./setup_bq_tables.sh PROJECT_ID DATASET_NAME`

#### `sample_data/setup_bq_tables.ps1` 🪟 WINDOWS SETUP
- **Purpose**: Automated setup for Windows
- **Features**: Same as Linux script
- **Usage**: `.\setup_bq_tables.ps1 -ProjectId "PROJECT" -DatasetName "DATASET"`

## Reading Order

### For First-Time Users
1. 📋 `SAMPLE_DATA_SUMMARY.md` - Get overview
2. 📖 `sample_data/README.md` - Follow setup steps
3. ⭐ Run framework with `sample_dq_config_comprehensive.json`
4. 📚 `sample_data/VALIDATION_SCENARIOS.md` - Learn validation types

### For Quick Setup
1. ⚡ `sample_data/QUICK_REFERENCE.md` - Get commands
2. 🐧/🪟 Run setup script
3. ⭐ Run framework

### For Developers
1. 📚 `sample_data/VALIDATION_SCENARIOS.md` - Understand all scenarios
2. 🗄️ `sample_data/bq_schemas.sql` - Review table structure
3. 📊 `sample_data/insert_data.sql` - Review test data
4. ⭐ `sample_dq_config_comprehensive.json` - Study configuration

## File Sizes (Approximate)

| File | Lines | Size | Type |
|------|-------|------|------|
| sample_dq_config_comprehensive.json | 350 | 15 KB | JSON |
| SAMPLE_DATA_SUMMARY.md | 350 | 15 KB | Markdown |
| sample_data/README.md | 200 | 8 KB | Markdown |
| sample_data/QUICK_REFERENCE.md | 200 | 8 KB | Markdown |
| sample_data/VALIDATION_SCENARIOS.md | 400 | 18 KB | Markdown |
| sample_data/bq_schemas.sql | 50 | 2 KB | SQL |
| sample_data/insert_data.sql | 80 | 5 KB | SQL |
| sample_data/setup_bq_tables.sh | 120 | 4 KB | Bash |
| sample_data/setup_bq_tables.ps1 | 130 | 5 KB | PowerShell |

## Key Features by File

### Configuration
- ✅ 20 validation checks
- ✅ All 5 validation types covered
- ✅ 19 active, 1 inactive check
- ✅ 18 pass, 1 fail (intentional)
- ✅ Realistic business scenarios

### Documentation
- ✅ Complete setup guide
- ✅ Quick reference card
- ✅ Comprehensive validation reference
- ✅ Troubleshooting tips
- ✅ Best practices

### Sample Data
- ✅ 5 tables, 57 rows
- ✅ Covers all validation scenarios
- ✅ Realistic business data
- ✅ Intentional test cases (null, duplicates)
- ✅ Multiple data types

### Automation
- ✅ Cross-platform setup scripts
- ✅ Automatic config updates
- ✅ Data verification
- ✅ Error handling
- ✅ Color-coded output

## Usage Patterns

### Pattern 1: Quick Start (5 minutes)
```bash
# 1. Run setup script
cd sample_data
./setup_bq_tables.sh my-project my-dataset

# 2. Run framework
cd ..
python main.py --config sample_dq_config_comprehensive.json
```

### Pattern 2: Manual Setup (10 minutes)
```bash
# 1. Create tables
bq query --use_legacy_sql=false < sample_data/bq_schemas.sql

# 2. Insert data
bq query --use_legacy_sql=false < sample_data/insert_data.sql

# 3. Update config (manual find/replace)
# Replace project.dataset with your values

# 4. Run framework
python main.py --config sample_dq_config_comprehensive.json
```

### Pattern 3: Learning Mode (30 minutes)
```bash
# 1. Read overview
cat SAMPLE_DATA_SUMMARY.md

# 2. Study validation scenarios
cat sample_data/VALIDATION_SCENARIOS.md

# 3. Review sample data
cat sample_data/insert_data.sql

# 4. Examine configuration
cat sample_dq_config_comprehensive.json

# 5. Run setup and test
cd sample_data
./setup_bq_tables.sh my-project my-dataset
cd ..
python main.py --config sample_dq_config_comprehensive.json
```

## Customization Points

### Add New Checks
Edit: `sample_dq_config_comprehensive.json`
- Add new check object
- Define SQL query
- Set expected output
- Choose comparison type

### Modify Sample Data
Edit: `sample_data/insert_data.sql`
- Add more rows
- Change values
- Add edge cases
- Test failure scenarios

### Add New Tables
Edit: `sample_data/bq_schemas.sql` and `sample_data/insert_data.sql`
- Define new table schema
- Insert sample data
- Create validation checks

### Extend Documentation
Edit any `.md` file
- Add examples
- Document edge cases
- Share best practices

## Dependencies

### Required
- Google Cloud SDK (gcloud, bq)
- Python 3.x
- BigQuery access

### Optional
- Bash (for .sh script)
- PowerShell (for .ps1 script)

## Support

For questions or issues:
1. Check `sample_data/README.md` troubleshooting section
2. Review `sample_data/VALIDATION_SCENARIOS.md` for validation details
3. Consult `sample_data/QUICK_REFERENCE.md` for common patterns

## Version History

- **v1.0** - Initial release with 20 validation checks
  - 5 tables with 57 rows
  - Complete documentation
  - Cross-platform setup scripts
  - All validation scenarios covered