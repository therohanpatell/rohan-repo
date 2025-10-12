# Negative Testing Package - Complete Index

## 📚 Quick Navigation

### 🚀 Getting Started (Read These First)

1. **[README_NEGATIVE_TESTING.md](README_NEGATIVE_TESTING.md)** ⭐ START HERE
   - Complete overview of the package
   - Quick start guide (3 steps)
   - File descriptions
   - Success criteria

2. **[NEGATIVE_TEST_QUICK_REFERENCE.md](NEGATIVE_TEST_QUICK_REFERENCE.md)** ⭐ CHEAT SHEET
   - One-page quick reference
   - Test commands
   - Expected results
   - Verification queries

3. **[NEGATIVE_TESTING_SUMMARY.md](NEGATIVE_TESTING_SUMMARY.md)** ⭐ OVERVIEW
   - Complete package summary
   - What you have
   - Quick reference to all files
   - Next actions

---

### 📖 Detailed Documentation

4. **[NEGATIVE_TEST_SCENARIOS.md](NEGATIVE_TEST_SCENARIOS.md)**
   - Detailed explanation of the 2 errors in mixed test
   - Expected system behavior
   - Phase-by-phase execution breakdown
   - Code implementation requirements

5. **[NEGATIVE_TEST_GUIDE.md](NEGATIVE_TEST_GUIDE.md)**
   - Comprehensive guide covering all 20 error types
   - Complete error message reference
   - Testing strategy and workflow
   - Implementation requirements
   - Troubleshooting guide

6. **[NEGATIVE_TEST_FLOW_DIAGRAM.md](NEGATIVE_TEST_FLOW_DIAGRAM.md)**
   - Visual flow diagrams
   - Old vs new behavior comparison
   - Execution timeline
   - Data flow diagrams

---

### 🧪 Test Files

7. **[sample_dq_config_negative_tests.json](sample_dq_config_negative_tests.json)** ⭐ PRIMARY TEST
   - 20 checks: 18 valid, 2 invalid
   - Realistic mixed scenario
   - Use this for main testing

8. **[sample_dq_config_all_negative_scenarios.json](sample_dq_config_all_negative_scenarios.json)**
   - 20 checks: 0 valid, 20 invalid
   - All error types covered
   - Use for comprehensive error testing

---

### 💻 Implementation Files

9. **[IMPLEMENTATION_EXAMPLE.py](IMPLEMENTATION_EXAMPLE.py)**
   - Example code for partial validation
   - Individual check validation pattern
   - Error handling and reporting
   - Expected output examples

10. **[test_negative_configs.py](test_negative_configs.py)** ⭐ TEST SCRIPT
    - Standalone test script
    - Validates config files without full pipeline
    - Colored output
    - Automated testing

---

## 🎯 Use Cases - Which File to Read

### "I want to get started quickly"
→ Read: **README_NEGATIVE_TESTING.md**  
→ Run: `python test_negative_configs.py`

### "I need quick commands and reference"
→ Read: **NEGATIVE_TEST_QUICK_REFERENCE.md**

### "I want to understand the 2 errors in detail"
→ Read: **NEGATIVE_TEST_SCENARIOS.md**

### "I need to know all 20 error types"
→ Read: **NEGATIVE_TEST_GUIDE.md**

### "I want to see visual diagrams"
→ Read: **NEGATIVE_TEST_FLOW_DIAGRAM.md**

### "I need to implement the solution"
→ Read: **IMPLEMENTATION_EXAMPLE.py**  
→ Reference: **NEGATIVE_TEST_GUIDE.md** (Implementation Requirements section)

### "I want to test my config files"
→ Run: `python test_negative_configs.py`

### "I want a complete overview"
→ Read: **NEGATIVE_TESTING_SUMMARY.md**

---

## 📋 File Details

### Documentation Files (7 files)

| File | Lines | Purpose | Priority |
|------|-------|---------|----------|
| README_NEGATIVE_TESTING.md | ~400 | Main documentation and quick start | ⭐⭐⭐ |
| NEGATIVE_TEST_QUICK_REFERENCE.md | ~300 | One-page cheat sheet | ⭐⭐⭐ |
| NEGATIVE_TESTING_SUMMARY.md | ~300 | Complete package overview | ⭐⭐⭐ |
| NEGATIVE_TEST_SCENARIOS.md | ~400 | Detailed 2-error explanation | ⭐⭐ |
| NEGATIVE_TEST_GUIDE.md | ~800 | Comprehensive 20-error guide | ⭐⭐ |
| NEGATIVE_TEST_FLOW_DIAGRAM.md | ~400 | Visual diagrams and flows | ⭐ |
| INDEX_NEGATIVE_TESTING.md | ~200 | This file - navigation index | ⭐ |

### Test Configuration Files (2 files)

| File | Checks | Valid | Invalid | Purpose | Priority |
|------|--------|-------|---------|---------|----------|
| sample_dq_config_negative_tests.json | 20 | 18 | 2 | Mixed scenario | ⭐⭐⭐ |
| sample_dq_config_all_negative_scenarios.json | 20 | 0 | 20 | All errors | ⭐⭐ |

### Implementation Files (2 files)

| File | Lines | Purpose | Priority |
|------|-------|---------|----------|
| IMPLEMENTATION_EXAMPLE.py | ~400 | Code examples and patterns | ⭐⭐⭐ |
| test_negative_configs.py | ~400 | Test script for validation | ⭐⭐⭐ |

---

## 🚀 Quick Start Workflow

### Step 1: Understand the Package
```bash
# Read the overview
cat README_NEGATIVE_TESTING.md

# Or read the summary
cat NEGATIVE_TESTING_SUMMARY.md
```

### Step 2: Test Configuration Files
```bash
# Test both config files
python test_negative_configs.py

# Expected output:
# ✅ PASS - Mixed scenario (18 valid, 2 invalid)
# ✅ PASS - All error scenarios (0 valid, 20 invalid)
```

### Step 3: Review Implementation
```bash
# Review the implementation example
cat IMPLEMENTATION_EXAMPLE.py

# Or open in your editor
code IMPLEMENTATION_EXAMPLE.py
```

### Step 4: Run Full Pipeline Test
```bash
# Run with mixed test config
python main.py --config sample_dq_config_negative_tests.json \
               --run-date 2025-10-12 \
               --target-table project.dataset.dq_results \
               --project-id your-project-id
```

### Step 5: Verify Results
```sql
-- Check BigQuery for results
SELECT COUNT(*) FROM `project.dataset.dq_results`
WHERE check_id LIKE 'CHK_NEG_%' 
  AND DATE(execution_timestamp) = CURRENT_DATE();
-- Should return 18
```

---

## 📊 Content Map

### By Topic

#### Validation Errors
- **2 Errors (Mixed Test):** NEGATIVE_TEST_SCENARIOS.md
- **20 Errors (Comprehensive):** NEGATIVE_TEST_GUIDE.md
- **Error Messages:** NEGATIVE_TEST_GUIDE.md (Error Messages Reference section)

#### Implementation
- **Code Examples:** IMPLEMENTATION_EXAMPLE.py
- **Implementation Pattern:** NEGATIVE_TEST_GUIDE.md (Implementation Requirements section)
- **Error Handling:** IMPLEMENTATION_EXAMPLE.py (load_and_validate_config_individually function)

#### Testing
- **Test Commands:** NEGATIVE_TEST_QUICK_REFERENCE.md
- **Test Script:** test_negative_configs.py
- **Test Strategy:** NEGATIVE_TEST_GUIDE.md (Testing Strategy section)
- **Verification:** NEGATIVE_TEST_QUICK_REFERENCE.md (Verification Queries section)

#### Visual Guides
- **Flow Diagrams:** NEGATIVE_TEST_FLOW_DIAGRAM.md
- **Old vs New:** NEGATIVE_TEST_FLOW_DIAGRAM.md (Comparison section)
- **Timeline:** NEGATIVE_TEST_FLOW_DIAGRAM.md (Execution Timeline section)

---

## 🎓 Learning Path

### Beginner Path (30 minutes)
1. Read: README_NEGATIVE_TESTING.md (10 min)
2. Read: NEGATIVE_TEST_QUICK_REFERENCE.md (5 min)
3. Run: `python test_negative_configs.py` (5 min)
4. Review: NEGATIVE_TEST_SCENARIOS.md (10 min)

### Intermediate Path (1 hour)
1. Complete Beginner Path (30 min)
2. Read: NEGATIVE_TEST_GUIDE.md (20 min)
3. Review: IMPLEMENTATION_EXAMPLE.py (10 min)

### Advanced Path (2 hours)
1. Complete Intermediate Path (1 hour)
2. Read: NEGATIVE_TEST_FLOW_DIAGRAM.md (15 min)
3. Implement: Update your code using patterns from IMPLEMENTATION_EXAMPLE.py (30 min)
4. Test: Run full pipeline with both test configs (15 min)

---

## 🔍 Search Guide

### Find Information About...

**"How to run tests"**
→ NEGATIVE_TEST_QUICK_REFERENCE.md (Test Commands section)

**"What are the 2 errors"**
→ NEGATIVE_TEST_SCENARIOS.md (Invalid Checks section)  
→ NEGATIVE_TEST_QUICK_REFERENCE.md (The 2 Errors section)

**"All 20 error types"**
→ NEGATIVE_TEST_GUIDE.md (All 20 Error Types section)  
→ NEGATIVE_TESTING_SUMMARY.md (All 20 Error Types table)

**"Expected error messages"**
→ NEGATIVE_TEST_GUIDE.md (Expected Error Messages Reference section)

**"How to implement"**
→ IMPLEMENTATION_EXAMPLE.py  
→ NEGATIVE_TEST_GUIDE.md (Implementation Requirements section)

**"Visual diagrams"**
→ NEGATIVE_TEST_FLOW_DIAGRAM.md

**"BigQuery verification"**
→ NEGATIVE_TEST_QUICK_REFERENCE.md (Verification Queries section)  
→ NEGATIVE_TEST_SCENARIOS.md (Verify Results section)

**"Success criteria"**
→ README_NEGATIVE_TESTING.md (Success Criteria section)  
→ NEGATIVE_TEST_GUIDE.md (Success Criteria section)

**"Troubleshooting"**
→ NEGATIVE_TEST_GUIDE.md (Troubleshooting section)  
→ README_NEGATIVE_TESTING.md (Troubleshooting section)

---

## 📞 Quick Reference

### Test Commands
```bash
# Test configs
python test_negative_configs.py

# Run mixed test
python main.py --config sample_dq_config_negative_tests.json ...

# Run all errors test
python main.py --config sample_dq_config_all_negative_scenarios.json ...
```

### Verification Query
```sql
SELECT COUNT(*) FROM `project.dataset.dq_results`
WHERE check_id LIKE 'CHK_NEG_%' 
  AND DATE(execution_timestamp) = CURRENT_DATE();
```

### Expected Results
- Mixed test: 18 rows in BigQuery, 2 validation errors
- All errors test: 0 rows in BigQuery, 20 validation errors

---

## 🎯 Key Files by Priority

### Must Read (⭐⭐⭐)
1. README_NEGATIVE_TESTING.md
2. NEGATIVE_TEST_QUICK_REFERENCE.md
3. NEGATIVE_TESTING_SUMMARY.md

### Should Read (⭐⭐)
4. NEGATIVE_TEST_SCENARIOS.md
5. NEGATIVE_TEST_GUIDE.md
6. IMPLEMENTATION_EXAMPLE.py

### Nice to Read (⭐)
7. NEGATIVE_TEST_FLOW_DIAGRAM.md
8. INDEX_NEGATIVE_TESTING.md (this file)

### Must Run (⭐⭐⭐)
9. test_negative_configs.py

### Must Test (⭐⭐⭐)
10. sample_dq_config_negative_tests.json

---

## 📦 Package Contents Summary

**Total Files:** 11
- Documentation: 7 files (~2,800 lines)
- Test Configs: 2 files (~1,000 lines)
- Implementation: 2 files (~800 lines)

**Total Content:** ~4,600 lines of comprehensive negative testing documentation and code

---

## ✨ What This Package Provides

✅ **Test Configuration Files**
- Mixed scenario (18 valid, 2 invalid)
- Comprehensive scenario (0 valid, 20 invalid)

✅ **Complete Documentation**
- Quick start guides
- Detailed error explanations
- Visual diagrams
- Implementation patterns

✅ **Implementation Examples**
- Code patterns for partial validation
- Error handling examples
- Expected output examples

✅ **Testing Tools**
- Standalone test script
- Verification queries
- Success criteria

✅ **Learning Resources**
- Multiple learning paths
- Search guide
- Quick reference cards

---

## 🎓 Next Steps

1. ✅ Read README_NEGATIVE_TESTING.md
2. ✅ Run `python test_negative_configs.py`
3. ✅ Review IMPLEMENTATION_EXAMPLE.py
4. ✅ Test with your pipeline
5. ✅ Verify in BigQuery

---

**Index Version:** 1.0  
**Created:** 2025-10-12  
**Last Updated:** 2025-10-12

---

**Need help?** Start with README_NEGATIVE_TESTING.md or NEGATIVE_TEST_QUICK_REFERENCE.md
