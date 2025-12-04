# ============================================
# Convert CSV to JSON Script
# ============================================
# Converts ALL_QUESTIONS_MASTER.csv to questions.json
# Usage: .\convert-csv-to-json.ps1
# ============================================

$csvFile = "ALL_QUESTIONS_MASTER.csv"
$jsonFile = "questions.json"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Converting CSV to JSON" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if CSV exists
if (-not (Test-Path $csvFile)) {
    Write-Host "ERROR: $csvFile not found!" -ForegroundColor Red
    exit 1
}

Write-Host "[1/3] Reading CSV file..." -ForegroundColor Yellow

# Read CSV and parse
$csvContent = Import-Csv -Path $csvFile

Write-Host "       Found $($csvContent.Count) questions" -ForegroundColor Gray

Write-Host "[2/3] Converting to JSON structure..." -ForegroundColor Yellow

# Convert to array of question objects
$questions = @()
$worldSet = @{}

foreach ($row in $csvContent) {
    $question = [PSCustomObject]@{
        world = $row.World
        questionNumber = [int]$row.Question_Number
        question = $row.Question
        options = @($row.Option_A, $row.Option_B, $row.Option_C, $row.Option_D)
        correctAnswer = $row.Correct_Answer
        difficulty = $row.Difficulty
    }
    $questions += $question
    $worldSet[$row.World] = $true
}

Write-Host "[3/3] Saving JSON file..." -ForegroundColor Yellow

# Convert to JSON and save
$jsonOutput = $questions | ConvertTo-Json -Depth 10
[System.IO.File]::WriteAllText($jsonFile, $jsonOutput, [System.Text.Encoding]::UTF8)

# Stats
$jsonSize = (Get-Item $jsonFile).Length
$worldCount = $worldSet.Keys.Count

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  SUCCESS! Created $jsonFile" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host "  Questions: $($questions.Count)" -ForegroundColor Gray
Write-Host "  Worlds: $worldCount" -ForegroundColor Gray
Write-Host "  File Size: $jsonSize bytes" -ForegroundColor Gray
Write-Host ""
