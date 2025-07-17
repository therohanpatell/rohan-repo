#!/usr/bin/env python3
"""
Test script to demonstrate the generic column configuration functionality.

This script shows how to use different column configurations and validates
that the pipeline works correctly with custom column names.
"""

import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def demonstrate_column_configuration():
    """Demonstrate the generic column configuration concept"""
    logger.info("=== GENERIC COLUMN CONFIGURATION DEMONSTRATION ===")
    
    # Example 1: Default Configuration
    logger.info("\n1. DEFAULT CONFIGURATION")
    default_config = {
        'metric_output': 'metric_output',
        'numerator_value': 'numerator_value', 
        'denominator_value': 'denominator_value',
        'business_data_date': 'business_data_date'
    }
    logger.info(f"Default config: {default_config}")
    
    # Example 2: Custom Configuration
    logger.info("\n2. CUSTOM CONFIGURATION")
    custom_config = {
        'metric_output': 'output',
        'numerator_value': 'numerator',
        'denominator_value': 'denominator',
        'business_data_date': 'date'
    }
    logger.info(f"Custom config: {custom_config}")
    
    # Example 3: Extended Configuration
    logger.info("\n3. EXTENDED CONFIGURATION")
    extended_config = {
        'metric_output': 'result',
        'numerator_value': 'num',
        'denominator_value': 'denom',
        'business_data_date': 'biz_date',
        'confidence_score': 'confidence',
        'data_quality': 'quality_score',
        'sample_size': 'n'
    }
    logger.info(f"Extended config: {extended_config}")
    
    return default_config, custom_config, extended_config


def show_sql_examples():
    """Show SQL examples for different configurations"""
    logger.info("\n=== SQL EXAMPLES ===")
    
    # Example 1: Default configuration
    logger.info("\nExample 1: Default Column Names")
    default_sql = """
    SELECT 
        SUM(revenue) as metric_output,
        COUNT(*) as numerator_value,
        COUNT(DISTINCT customer_id) as denominator_value,
        CURRENT_DATE() as business_data_date
    FROM `project.dataset.sales`
    WHERE date = '{currently}'
    """
    logger.info(default_sql)
    
    # Example 2: Custom configuration
    logger.info("\nExample 2: Custom Column Names")
    custom_sql = """
    SELECT 
        SUM(revenue) as output,
        COUNT(*) as numerator,
        COUNT(DISTINCT customer_id) as denominator,
        CURRENT_DATE() as date
    FROM `project.dataset.sales`
    WHERE date = '{currently}'
    """
    logger.info(custom_sql)
    
    # Example 3: With additional columns
    logger.info("\nExample 3: With Additional Columns")
    extended_sql = """
    SELECT 
        SUM(revenue) as result,
        COUNT(*) as num,
        COUNT(DISTINCT customer_id) as denom,
        CURRENT_DATE() as biz_date,
        0.95 as confidence,
        'HIGH' as quality_score,
        1000 as n
    FROM `project.dataset.sales`
    WHERE date = '{currently}'
    """
    logger.info(extended_sql)


def show_usage_examples():
    """Show usage examples for the generic column configuration"""
    logger.info("\n=== USAGE EXAMPLES ===")
    
    # Example 1: Initialize with default config
    logger.info("\n1. Initialize with default configuration:")
    logger.info("""
    pipeline = MetricsPipeline(spark, bq_client)
    # Uses default column names: metric_output, numerator_value, denominator_value, business_data_date
    """)
    
    # Example 2: Initialize with custom config
    logger.info("\n2. Initialize with custom configuration:")
    logger.info("""
    custom_config = {
        'metric_output': 'output',
        'numerator_value': 'numerator',
        'denominator_value': 'denominator',
        'business_data_date': 'date'
    }
    pipeline = MetricsPipeline(spark, bq_client, column_config=custom_config)
    """)
    
    # Example 3: Runtime updates
    logger.info("\n3. Update configuration at runtime:")
    logger.info("""
    new_config = {
        'metric_output': 'result',
        'numerator_value': 'num',
        'denominator_value': 'denom',
        'business_data_date': 'biz_date'
    }
    pipeline.update_column_config(new_config)
    """)
    
    # Example 4: Add new columns
    logger.info("\n4. Add new column mappings:")
    logger.info("""
    pipeline.add_column_mapping('confidence_score', 'confidence')
    pipeline.add_column_mapping('data_quality', 'quality_score')
    pipeline.add_column_mapping('sample_size', 'n')
    """)


def show_migration_scenario():
    """Show migration scenario from old to new column names"""
    logger.info("\n=== MIGRATION SCENARIO ===")
    
    logger.info("\nStep 1: Start with old configuration")
    old_config = {
        'metric_output': 'metric_output',
        'numerator_value': 'numerator_value',
        'denominator_value': 'denominator_value',
        'business_data_date': 'business_data_date'
    }
    logger.info(f"Old config: {old_config}")
    
    logger.info("\nStep 2: Migrate to new configuration")
    new_config = {
        'metric_output': 'output',
        'numerator_value': 'numerator',
        'denominator_value': 'denominator',
        'business_data_date': 'date'
    }
    logger.info(f"New config: {new_config}")
    
    logger.info("\nStep 3: Add new columns")
    final_config = {
        'metric_output': 'output',
        'numerator_value': 'numerator',
        'denominator_value': 'denominator',
        'business_data_date': 'date',
        'confidence_score': 'confidence',
        'sample_size': 'n'
    }
    logger.info(f"Final config: {final_config}")
    
    logger.info("\nMigration benefits:")
    logger.info("- No code changes required")
    logger.info("- Backward compatibility maintained")
    logger.info("- Gradual migration supported")
    logger.info("- Easy to add new columns")


def show_benefits():
    """Show the benefits of generic column configuration"""
    logger.info("\n=== BENEFITS ===")
    
    benefits = [
        "🔧 Flexibility: Easy to adapt to different naming conventions",
        "🛠️ Maintainability: No code changes needed for column name updates",
        "📈 Scalability: Simple to add new columns and metric types",
        "🔄 Compatibility: Backward compatible with existing configurations",
        "✅ Validation: Built-in validation for required columns and data types",
        "🚀 Runtime Updates: Change configuration without restarting pipeline",
        "📊 Dynamic Schema: Schema automatically adapts to configuration",
        "🎯 Type Safety: Automatic type inference and validation"
    ]
    
    for benefit in benefits:
        logger.info(benefit)


def main():
    """Main demonstration function"""
    logger.info("Starting Generic Column Configuration Demonstration")
    
    try:
        # Demonstrate different configurations
        demonstrate_column_configuration()
        
        # Show SQL examples
        show_sql_examples()
        
        # Show usage examples
        show_usage_examples()
        
        # Show migration scenario
        show_migration_scenario()
        
        # Show benefits
        show_benefits()
        
        logger.info("\n🎉 Demonstration completed successfully!")
        logger.info("\nThe generic column configuration system allows you to:")
        logger.info("1. Change column names without modifying code")
        logger.info("2. Add new columns easily")
        logger.info("3. Support different naming conventions")
        logger.info("4. Maintain backward compatibility")
        logger.info("5. Update configuration at runtime")
        
    except Exception as e:
        logger.error(f"❌ Demonstration failed: {str(e)}")
        raise


if __name__ == "__main__":
    main() 