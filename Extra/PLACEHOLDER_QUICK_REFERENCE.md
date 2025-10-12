# Placeholder Quick Reference Card

## 🎯 Two Placeholders Available

| Placeholder | Replaced With | Requires | Example |
|-------------|---------------|----------|---------|
| `{currently}` | `--run-date` value | Nothing | `WHERE date = {currently}` |
| `{partition_info}` | Latest partition from metadata | `--dq-partition-info-table` | `WHERE partition_dt = {partition_info}` |

---

## 🚀 Quick Commands

### With {currently} Only
```bash
python main.py --mode dq \
               --gcs-path gs://bucket/config.json \
               --run-date 2025-10-12 \
               --dq-target-table project.dataset.dq_results
```

### With {partition_info}
```bash
python main.py --mode dq \
               --gcs-path gs://bucket/config.json \
               --run-date 2025-10-12 \
               --dq-partition-info-table project.dataset.partition_info \
               --dq-target-table project.dataset.dq_results
```

---

## 📝 Quick Examples

### Example 1: {currently}
```json
{
  "sql_query": "SELECT COUNT(*) FROM table WHERE date = {currently}",
  "expected_output": ">0"
}
```
**Result:** `WHERE date = '2025-10-12'`

### Example 2: {partition_info}
```json
{
  "sql_query": "SELECT COUNT(*) FROM table WHERE partition_dt = {partition_info}",
  "expected_output": ">0"
}
```
**Result:** `WHERE partition_dt = '2025-10-11'` (from metadata)

### Example 3: Both
```json
{
  "sql_query": "SELECT COUNT(*) FROM table WHERE date = {currently} AND partition_dt = {partition_info}",
  "expected_output": ">=100"
}
```
**Result:** `WHERE date = '2025-10-12' AND partition_dt = '2025-10-11'`

---

## 🚨 Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| "no partition_info_table provided" | Missing argument | Add `--dq-partition-info-table` |
| "Could not find partition_dt" | No metadata | Insert partition metadata |
| "Could not find table reference" | No FROM clause | Add `FROM table` before placeholder |

---

## ✅ Checklist

- [ ] Replace hardcoded dates with `{currently}`
- [ ] Replace hardcoded partitions with `{partition_info}`
- [ ] Add `--dq-partition-info-table` if using `{partition_info}`
- [ ] Test with sample config
- [ ] Verify placeholders in logs

---

## 📚 Full Documentation

- **Complete Guide:** `DQ_PLACEHOLDER_GUIDE.md`
- **Implementation:** `PLACEHOLDER_IMPLEMENTATION_SUMMARY.md`
- **Sample Config:** `sample_dq_config_with_placeholders.json`

---

**Quick Tip:** Placeholders are optional - static dates still work!
