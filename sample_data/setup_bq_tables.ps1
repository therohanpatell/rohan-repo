# DQ Validation Framework - BigQuery Setup Script (PowerShell)
# This script creates tables and inserts sample data into BigQuery

param(
    [Parameter(Mandatory=$true)]
    [string]$ProjectId,
    
    [Parameter(Mandatory=$true)]
    [string]$DatasetName
)

# Function to print colored output
function Write-Info {
    param([string]$Message)
    Write-Host "[INFO] $Message" -ForegroundColor Green
}

function Write-Error-Custom {
    param([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

function Write-Warning-Custom {
    param([string]$Message)
    Write-Host "[WARNING] $Message" -ForegroundColor Yellow
}

Write-Info "Setting up DQ Validation Framework sample data"
Write-Info "Project: $ProjectId"
Write-Info "Dataset: $DatasetName"

# Check if bq command is available
try {
    $null = Get-Command bq -ErrorAction Stop
} catch {
    Write-Error-Custom "bq command not found. Please install Google Cloud SDK."
    exit 1
}

# Set the project
Write-Info "Setting active project..."
gcloud config set project $ProjectId

# Check if dataset exists, create if not
Write-Info "Checking if dataset exists..."
$datasetExists = bq ls -d "$ProjectId`:$DatasetName" 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Warning-Custom "Dataset $DatasetName already exists. Using existing dataset."
} else {
    Write-Info "Creating dataset $DatasetName..."
    bq mk --dataset "$ProjectId`:$DatasetName"
    if ($LASTEXITCODE -eq 0) {
        Write-Info "Dataset created successfully."
    } else {
        Write-Error-Custom "Failed to create dataset."
        exit 1
    }
}

# Create temporary SQL files with updated project and dataset names
Write-Info "Preparing SQL scripts..."
$schemasContent = Get-Content "bq_schemas.sql" -Raw
$schemasContent = $schemasContent -replace "project\.dataset", "$ProjectId.$DatasetName"
$schemasContent | Out-File -FilePath "$env:TEMP\bq_schemas_temp.sql" -Encoding UTF8

$insertContent = Get-Content "insert_data.sql" -Raw
$insertContent = $insertContent -replace "project\.dataset", "$ProjectId.$DatasetName"
$insertContent | Out-File -FilePath "$env:TEMP\insert_data_temp.sql" -Encoding UTF8

# Create tables
Write-Info "Creating tables..."
Get-Content "$env:TEMP\bq_schemas_temp.sql" | bq query --use_legacy_sql=false
if ($LASTEXITCODE -eq 0) {
    Write-Info "Tables created successfully."
} else {
    Write-Error-Custom "Failed to create tables."
    exit 1
}

# Insert data
Write-Info "Inserting sample data..."
Get-Content "$env:TEMP\insert_data_temp.sql" | bq query --use_legacy_sql=false
if ($LASTEXITCODE -eq 0) {
    Write-Info "Sample data inserted successfully."
} else {
    Write-Error-Custom "Failed to insert sample data."
    exit 1
}

# Verify data
Write-Info "Verifying data insertion..."
$verifyQuery = @"
SELECT 'employees' as table_name, COUNT(*) as row_count FROM ``$ProjectId.$DatasetName.employees``
UNION ALL
SELECT 'orders', COUNT(*) FROM ``$ProjectId.$DatasetName.orders``
UNION ALL
SELECT 'sales', COUNT(*) FROM ``$ProjectId.$DatasetName.sales``
UNION ALL
SELECT 'customers', COUNT(*) FROM ``$ProjectId.$DatasetName.customers``
UNION ALL
SELECT 'products', COUNT(*) FROM ``$ProjectId.$DatasetName.products``
ORDER BY table_name;
"@

$verifyQuery | bq query --use_legacy_sql=false --format=pretty

# Update configuration file
Write-Info "Updating DQ configuration file..."
Set-Location ..
if (Test-Path "sample_dq_config_comprehensive.json") {
    # Create backup
    Copy-Item "sample_dq_config_comprehensive.json" "sample_dq_config_comprehensive.json.bak"
    
    # Update file
    $configContent = Get-Content "sample_dq_config_comprehensive.json" -Raw
    $configContent = $configContent -replace "project\.dataset", "$ProjectId.$DatasetName"
    $configContent | Out-File -FilePath "sample_dq_config_comprehensive.json" -Encoding UTF8
    
    Write-Info "Configuration file updated: sample_dq_config_comprehensive.json"
    Write-Info "Backup created: sample_dq_config_comprehensive.json.bak"
} else {
    Write-Warning-Custom "Configuration file not found. Please update manually."
}

# Clean up temporary files
Remove-Item "$env:TEMP\bq_schemas_temp.sql" -ErrorAction SilentlyContinue
Remove-Item "$env:TEMP\insert_data_temp.sql" -ErrorAction SilentlyContinue

Write-Info "Setup completed successfully!"
Write-Host ""
Write-Info "Next steps:"
Write-Host "  1. Review the configuration file: sample_dq_config_comprehensive.json"
Write-Host "  2. Run the DQ validation framework:"
Write-Host "     python main.py --config sample_dq_config_comprehensive.json"
Write-Host ""
Write-Info "Expected results:"
Write-Host "  - 19 active checks"
Write-Host "  - 18 checks should PASS"
Write-Host "  - 1 check should FAIL (CHK_001 - null employee_id)"
