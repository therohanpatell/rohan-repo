# Optimized Metrics Pipeline

An optimized, modular, and maintainable version of the PySpark BigQuery metrics pipeline with reduced code duplication and improved architecture.

## Overview

This pipeline has been completely refactored from the original `pysaprk.py` to provide:

- **Modular Architecture**: Separated concerns into distinct modules
- **Reduced Code Duplication**: Common patterns extracted into reusable utilities
- **Better Error Handling**: Comprehensive exception hierarchy
- **Improved Maintainability**: Clear separation of responsibilities
- **Configuration Management**: Centralized configuration system
- **Enhanced Logging**: Consistent logging across all components

## Architecture

```
optimized_metrics_pipeline/
├── config/                 # Configuration management
│   ├── __init__.py
│   ├── constants.py       # Constants and schemas
│   └── settings.py        # Configuration classes
├── utils/                 # Common utilities
│   ├── __init__.py
│   ├── exceptions.py      # Custom exceptions
│   ├── logging.py         # Logging utilities
│   ├── spark_session.py   # Spark session management
│   ├── numeric_utils.py   # Numeric processing utilities
│   └── date_utils.py      # Date handling utilities
├── validation/            # Data validation
│   ├── __init__.py
│   ├── json_validator.py  # JSON data validation
│   ├── gcs_validator.py   # GCS operations validation
│   └── data_validator.py  # Business rules validation
├── bigquery/             # BigQuery operations
│   ├── __init__.py
│   ├── client.py         # BigQuery client wrapper
│   ├── schema_manager.py # Schema management
│   ├── writer.py         # Writing operations
│   └── reader.py         # Reading operations
├── sql/                  # SQL processing
│   ├── __init__.py
│   └── placeholder_manager.py  # SQL placeholder handling
├── metrics/              # Metrics processing
│   ├── __init__.py
│   └── processor.py      # Metrics execution logic
├── recon/                # Reconciliation
│   ├── __init__.py
│   └── manager.py        # Recon record management
├── main.py              # Main pipeline orchestrator
└── README.md           # This documentation
```

## Key Improvements

### 1. **Modular Design**
- Each module has a single responsibility
- Easy to test individual components
- Clear interfaces between modules

### 2. **Code Deduplication**
- Common validation logic extracted to utilities
- Reusable BigQuery operations
- Shared SQL processing patterns

### 3. **Better Error Handling**
- Custom exception hierarchy
- Consistent error reporting
- Improved rollback mechanisms

### 4. **Configuration Management**
- Centralized configuration system
- Environment-specific settings
- Easy to modify without code changes

### 5. **Enhanced Logging**
- Consistent logging format
- Configurable log levels
- Better debugging information

## Usage

### Basic Usage

```bash
python -m optimized_metrics_pipeline.main \
    --gcs_path gs://bucket/metrics.json \
    --run_date 2024-01-01 \
    --dependencies dep1,dep2 \
    --partition_info_table project.dataset.partition_info \
    --env PRD \
    --recon_table project.dataset.recon
```

### Configuration

The pipeline uses a configuration system that can be customized:

```python
from optimized_metrics_pipeline.config import PipelineConfig

config = PipelineConfig(
    log_level="DEBUG",
    validate_denominator=True,
    enable_rollback=True,
    rollback_timeout_hours=2
)
```

## Components

### Configuration (`config/`)

- **constants.py**: All constants, schemas, and error messages
- **settings.py**: Configuration classes with type hints

### Utilities (`utils/`)

- **exceptions.py**: Custom exception hierarchy
- **logging.py**: Logging utilities and mixins
- **spark_session.py**: Spark session management
- **numeric_utils.py**: Numeric precision handling
- **date_utils.py**: Date formatting and validation

### Validation (`validation/`)

- **json_validator.py**: JSON structure validation
- **gcs_validator.py**: GCS path and file validation
- **data_validator.py**: Business rules validation

### BigQuery (`bigquery/`)

- **client.py**: Enhanced BigQuery client
- **schema_manager.py**: Schema alignment and management
- **writer.py**: Writing operations with transaction safety
- **reader.py**: Reading operations and queries

### SQL (`sql/`)

- **placeholder_manager.py**: SQL placeholder replacement logic

### Metrics (`metrics/`)

- **processor.py**: Metrics execution and DataFrame creation

### Reconciliation (`recon/`)

- **manager.py**: Reconciliation record creation and management

## Key Features

### 1. **Transaction Safety**
- Rollback capabilities for failed operations
- Tracking of processed vs overwritten metrics
- Timeout-based rollback operations

### 2. **Precision Handling**
- Decimal precision preservation
- Numeric validation and conversion
- BigQuery-compatible data types

### 3. **Schema Management**
- Automatic schema alignment
- Type conversion for BigQuery compatibility
- Schema validation and suggestions

### 4. **Comprehensive Logging**
- Structured logging with consistent format
- Configurable log levels
- Performance and execution statistics

### 5. **Error Recovery**
- Graceful handling of individual metric failures
- Partial processing continuation
- Comprehensive error reporting

## Benefits Over Original Code

1. **Maintainability**: Clear separation of concerns makes the code easier to understand and modify
2. **Testability**: Modular design enables unit testing of individual components
3. **Reusability**: Common patterns are extracted into reusable utilities
4. **Extensibility**: Easy to add new features without modifying existing code
5. **Reliability**: Better error handling and rollback mechanisms
6. **Performance**: Optimized BigQuery operations and Spark usage
7. **Debugging**: Enhanced logging and error reporting

## Dependencies

- PySpark
- Google Cloud BigQuery
- Python 3.7+

## Installation

```bash
pip install pyspark google-cloud-bigquery
```

## Development

### Running Tests

```bash
# Run unit tests (if test files were created)
python -m pytest tests/
```

### Adding New Features

1. Create new modules in appropriate directories
2. Follow the existing patterns for logging and error handling
3. Update configuration if needed
4. Add to the main orchestrator

## Migration from Original Code

The optimized pipeline maintains the same functionality as the original `pysaprk.py` but with:

- **Same CLI interface**: All command-line arguments remain the same
- **Same data flow**: JSON → Validation → Processing → BigQuery → Recon
- **Same output format**: BigQuery tables have the same structure
- **Same placeholder logic**: `{currently}` and `{partition_info}` work identically

## Contributing

1. Follow the modular architecture patterns
2. Add appropriate logging and error handling
3. Update configuration if adding new settings
4. Maintain backward compatibility

## License

This optimized pipeline maintains the same license as the original code.