# Metrics Pipeline Framework - Team Documentation

Welcome to the Metrics Pipeline Framework documentation! This guide will help you understand and use the framework effectively.

## 📚 Documentation Overview

This documentation set includes three main documents designed for different use cases:

### 1. [Comprehensive Documentation](METRICS_PIPELINE_FRAMEWORK_DOCUMENTATION.md) 📖
**For: In-depth learning and reference**

The complete guide covering:
- Framework architecture and design
- Detailed component explanations
- Step-by-step setup instructions
- Comprehensive examples
- Advanced features and customization
- Troubleshooting guides
- Best practices

**Use when:** You need to understand how the framework works, setting up for the first time, or solving complex issues.

### 2. [Quick Reference Guide](METRICS_PIPELINE_QUICK_REFERENCE.md) ⚡
**For: Daily operations and quick lookups**

The cheat sheet containing:
- Command templates
- JSON configuration examples
- SQL query patterns
- Common error fixes
- Performance tips
- Monitoring queries

**Use when:** You need quick answers, command examples, or troubleshooting common issues.

### 3. [Framework Code](pysaprk.py) 💻
**For: Implementation details**

The main Python file with:
- Complete framework implementation
- Class and method documentation
- Error handling logic
- All supported features

**Use when:** You need to understand implementation details or extend the framework.

## 🚀 Getting Started

### For New Team Members
1. **Start with:** [Comprehensive Documentation](METRICS_PIPELINE_FRAMEWORK_DOCUMENTATION.md)
2. **Read sections:** Overview, Architecture, Prerequisites, Getting Started
3. **Practice with:** Configuration and Usage Examples
4. **Keep handy:** [Quick Reference Guide](METRICS_PIPELINE_QUICK_REFERENCE.md)

### For Experienced Users
1. **Quick lookup:** [Quick Reference Guide](METRICS_PIPELINE_QUICK_REFERENCE.md)
2. **Advanced features:** [Comprehensive Documentation](METRICS_PIPELINE_FRAMEWORK_DOCUMENTATION.md) - Advanced Features section
3. **Troubleshooting:** Both documents have troubleshooting sections

## 📋 Common Workflows

### Setting Up a New Metric
1. Check [JSON Configuration Template](METRICS_PIPELINE_QUICK_REFERENCE.md#-json-configuration-template)
2. Review [SQL Requirements](METRICS_PIPELINE_QUICK_REFERENCE.md#-sql-requirements)
3. Test with [Usage Examples](METRICS_PIPELINE_FRAMEWORK_DOCUMENTATION.md#usage-examples)
4. Deploy following [Best Practices](METRICS_PIPELINE_FRAMEWORK_DOCUMENTATION.md#best-practices)

### Troubleshooting Issues
1. Check [Common Issues & Quick Fixes](METRICS_PIPELINE_QUICK_REFERENCE.md#️-common-issues--quick-fixes)
2. Review [Error Message Lookup](METRICS_PIPELINE_QUICK_REFERENCE.md#error-message-lookup)
3. Consult [Troubleshooting Guide](METRICS_PIPELINE_FRAMEWORK_DOCUMENTATION.md#troubleshooting)
4. Enable debug logging for detailed investigation

### Performance Optimization
1. Review [Performance Tips](METRICS_PIPELINE_QUICK_REFERENCE.md#-performance-tips)
2. Check [SQL Best Practices](METRICS_PIPELINE_FRAMEWORK_DOCUMENTATION.md#best-practices)
3. Monitor using [Monitoring Queries](METRICS_PIPELINE_QUICK_REFERENCE.md#-monitoring--logs)

## 🛠️ Framework Features

### Core Capabilities
- **Dynamic SQL Processing**: Placeholder replacement with `{currently}` and `{partition_info}`
- **Quote Normalization**: Automatic SQL quote handling for BigQuery compatibility
- **Schema Alignment**: Automatic DataFrame to BigQuery schema matching
- **Error Handling**: Comprehensive error management with rollback capabilities
- **Audit Trail**: Complete reconciliation records for all processed metrics
- **Overwrite Protection**: Safe metric updates with existing data management

### Integration Points
- **Google Cloud Storage**: JSON configuration file storage
- **BigQuery**: Target tables and reconciliation data
- **PySpark**: Distributed processing engine
- **Logging**: Comprehensive logging and monitoring

## 🔧 Quick Commands

### Basic Usage
```bash
python pysaprk.py \
  --gcs_path "gs://bucket/config.json" \
  --run_date "2024-01-15" \
  --dependencies "daily_metrics" \
  --partition_info_table "project.dataset.partition_info" \
  --env "PROD" \
  --recon_table "project.dataset.recon"
```

### Development Testing
```bash
python pysaprk.py \
  --gcs_path "gs://dev-bucket/test-config.json" \
  --run_date "2024-01-15" \
  --dependencies "test_metrics" \
  --partition_info_table "dev-project.test.partition_info" \
  --env "DEV" \
  --recon_table "dev-project.test.recon"
```

## 📊 Monitoring

### Check Pipeline Status
```sql
SELECT 
  rcncln_exact_pass_in as status,
  COUNT(*) as count
FROM `project.dataset.recon`
WHERE schdld_dt = '2024-01-15'
GROUP BY rcncln_exact_pass_in
```

### View Recent Metrics
```sql
SELECT 
  source_system_id as metric_id,
  Job_Name as metric_name,
  rcncln_exact_pass_in as status,
  load_ts
FROM `project.dataset.recon`
WHERE schdld_dt = '2024-01-15'
ORDER BY load_ts DESC
```

## ⚠️ Important Notes

### For Fresh Team Members
- **Always test in DEV environment first**
- **Review both documentation files before starting**
- **Keep the Quick Reference handy while working**
- **Check reconciliation table after each run**
- **Read the expanded FAQ section** - Contains 50+ questions covering code concepts, development setup, SQL, error handling, and production deployment
- **Use the beginner's code concepts** in Quick Reference for understanding Python and PySpark patterns
- **Ask questions if anything is unclear** - Most common questions are already covered in the FAQ

### Production Considerations
- **Backup critical data before running**
- **Monitor resource usage during execution**
- **Check logs for warnings or errors**
- **Validate results in target tables**
- **Review reconciliation records for audit**

## 📞 Getting Help

### Self-Service Resources
1. **Quick fixes**: [Quick Reference Guide](METRICS_PIPELINE_QUICK_REFERENCE.md#️-common-issues--quick-fixes)
2. **Detailed troubleshooting**: [Comprehensive Documentation](METRICS_PIPELINE_FRAMEWORK_DOCUMENTATION.md#troubleshooting)
3. **FAQ**: [Comprehensive Documentation](METRICS_PIPELINE_FRAMEWORK_DOCUMENTATION.md#faq)

### When to Ask for Help
- Error messages not covered in documentation
- Framework behavior seems incorrect
- Performance issues persist after optimization
- Need to extend framework functionality

### Information to Include
- Full error logs
- JSON configuration used
- Command executed
- Environment details (DEV/PROD)
- Expected vs actual behavior

---

## 📈 Framework Benefits

### For Development Teams
- **Faster onboarding** with comprehensive documentation
- **Consistent patterns** across all metrics
- **Error prevention** through validation and best practices
- **Easy troubleshooting** with detailed guides

### For Operations
- **Reliable processing** with rollback capabilities
- **Complete audit trail** through reconciliation records
- **Performance monitoring** with built-in logging
- **Scalable architecture** using PySpark

### For Business
- **Consistent metrics** across all systems
- **Timely processing** with automated pipelines
- **Quality assurance** through validation
- **Compliance support** with audit trails

---

**Happy Processing!** 🎉

*This framework is designed to make metrics processing easier and more reliable. Don't hesitate to refer to the documentation whenever you need help.* 