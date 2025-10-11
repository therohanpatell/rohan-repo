#!/bin/bash

# DQ Validation Framework - BigQuery Setup Script
# This script creates tables and inserts sample data into BigQuery

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check if required parameters are provided
if [ $# -ne 2 ]; then
    print_error "Usage: $0 <project_id> <dataset_name>"
    echo "Example: $0 my-project my-dataset"
    exit 1
fi

PROJECT_ID=$1
DATASET_NAME=$2

print_info "Setting up DQ Validation Framework sample data"
print_info "Project: $PROJECT_ID"
print_info "Dataset: $DATASET_NAME"

# Check if bq command is available
if ! command -v bq &> /dev/null; then
    print_error "bq command not found. Please install Google Cloud SDK."
    exit 1
fi

# Set the project
print_info "Setting active project..."
gcloud config set project $PROJECT_ID

# Check if dataset exists, create if not
print_info "Checking if dataset exists..."
if bq ls -d $PROJECT_ID:$DATASET_NAME &> /dev/null; then
    print_warning "Dataset $DATASET_NAME already exists. Using existing dataset."
else
    print_info "Creating dataset $DATASET_NAME..."
    bq mk --dataset $PROJECT_ID:$DATASET_NAME
    if [ $? -eq 0 ]; then
        print_info "Dataset created successfully."
    else
        print_error "Failed to create dataset."
        exit 1
    fi
fi

# Create temporary SQL files with updated project and dataset names
print_info "Preparing SQL scripts..."
sed "s/project\.dataset/$PROJECT_ID.$DATASET_NAME/g" bq_schemas.sql > /tmp/bq_schemas_temp.sql
sed "s/project\.dataset/$PROJECT_ID.$DATASET_NAME/g" insert_data.sql > /tmp/insert_data_temp.sql

# Create tables
print_info "Creating tables..."
bq query --use_legacy_sql=false < /tmp/bq_schemas_temp.sql
if [ $? -eq 0 ]; then
    print_info "Tables created successfully."
else
    print_error "Failed to create tables."
    exit 1
fi

# Insert data
print_info "Inserting sample data..."
bq query --use_legacy_sql=false < /tmp/insert_data_temp.sql
if [ $? -eq 0 ]; then
    print_info "Sample data inserted successfully."
else
    print_error "Failed to insert sample data."
    exit 1
fi

# Verify data
print_info "Verifying data insertion..."
VERIFY_QUERY="
SELECT 'employees' as table_name, COUNT(*) as row_count FROM \`$PROJECT_ID.$DATASET_NAME.employees\`
UNION ALL
SELECT 'orders', COUNT(*) FROM \`$PROJECT_ID.$DATASET_NAME.orders\`
UNION ALL
SELECT 'sales', COUNT(*) FROM \`$PROJECT_ID.$DATASET_NAME.sales\`
UNION ALL
SELECT 'customers', COUNT(*) FROM \`$PROJECT_ID.$DATASET_NAME.customers\`
UNION ALL
SELECT 'products', COUNT(*) FROM \`$PROJECT_ID.$DATASET_NAME.products\`
ORDER BY table_name;
"

echo "$VERIFY_QUERY" | bq query --use_legacy_sql=false --format=pretty

# Update configuration file
print_info "Updating DQ configuration file..."
cd ..
if [ -f "sample_dq_config_comprehensive.json" ]; then
    sed -i.bak "s/project\.dataset/$PROJECT_ID.$DATASET_NAME/g" sample_dq_config_comprehensive.json
    print_info "Configuration file updated: sample_dq_config_comprehensive.json"
    print_info "Backup created: sample_dq_config_comprehensive.json.bak"
else
    print_warning "Configuration file not found. Please update manually."
fi

# Clean up temporary files
rm -f /tmp/bq_schemas_temp.sql /tmp/insert_data_temp.sql

print_info "Setup completed successfully!"
echo ""
print_info "Next steps:"
echo "  1. Review the configuration file: sample_dq_config_comprehensive.json"
echo "  2. Run the DQ validation framework:"
echo "     python main.py --config sample_dq_config_comprehensive.json"
echo ""
print_info "Expected results:"
echo "  - 19 active checks"
echo "  - 18 checks should PASS"
echo "  - 1 check should FAIL (CHK_001 - null employee_id)"
