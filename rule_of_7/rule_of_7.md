# Headcount Masking Test Scenarios

## Test Scenario 1: Basic Stage 1 Masking
**Purpose**: Verify that all records with headcount < 7 are masked at Stage 1

**Input Data**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 5         | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 12        | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 20        | NULL            | NULL            |

**Expected Output**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 5         | **Yes**         | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 12        | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 20        | NULL            | NULL            |

**Validation**:
- ✓ Record with headcount 5 should have stage_1_masking = "Yes"
- ✓ Records with headcount ≥ 7 should have stage_1_masking = NULL

---

## Test Scenario 2: Stage 2 Masking - Qualifying Group
**Purpose**: Verify Stage 2 masking is applied when group has exactly one low-headcount record

**Input Data**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 4         | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 8         | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 15        | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 25        | NULL            | NULL            |

**Expected Output**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 4         | **Yes**         | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 8         | NULL            | **Yes**         |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 15        | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 25        | NULL            | NULL            |

**Validation**:
- ✓ Group has exactly 1 record with headcount < 7 (record with HC=4)
- ✓ Record with HC=4 should have stage_1_masking = "Yes"
- ✓ Record with HC=8 (second lowest, lowest ≥ 7) should have stage_2_masking = "Yes"
- ✓ Records with HC=15 and HC=25 should remain unmasked

---

## Test Scenario 3: Stage 2 NOT Applied - Multiple Low Headcounts
**Purpose**: Verify Stage 2 masking is NOT applied when group has 2+ low-headcount records

**Input Data**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 3         | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 5         | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 12        | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 20        | NULL            | NULL            |

**Expected Output**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 3         | **Yes**         | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 5         | **Yes**         | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 12        | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 20        | NULL            | NULL            |

**Validation**:
- ✓ Group has 2 records with headcount < 7 (HC=3 and HC=5)
- ✓ Both low-headcount records should have stage_1_masking = "Yes"
- ✓ NO records should have stage_2_masking = "Yes" (Stage 2 excluded)
- ✓ Records with HC=12 and HC=20 should remain unmasked

---

## Test Scenario 4: Different Groups - Same Hierarchy, Different Region
**Purpose**: Verify that records with same org hierarchy but different region/location are treated as separate groups

**Input Data**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 4         | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 10        | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | LA       | 5         | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | LA       | 8         | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | LA       | 15        | NULL            | NULL            |

**Expected Output**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 4         | **Yes**         | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 10        | NULL            | **Yes**         |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | LA       | 5         | **Yes**         | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | LA       | 8         | NULL            | **Yes**         |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | LA       | 15        | NULL            | NULL            |

**Validation**:
- ✓ NYC group (location=NYC): 1 low HC → Stage 2 applied to HC=10
- ✓ LA group (location=LA): 1 low HC → Stage 2 applied to HC=8
- ✓ Groups are independent despite same org_level values

---

## Test Scenario 5: Multiple Records with Same Second-Lowest Headcount
**Purpose**: Verify that ALL records with the second-lowest headcount value are masked

**Input Data**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 6         | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 10        | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 10        | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 10        | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 25        | NULL            | NULL            |

**Expected Output**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 6         | **Yes**         | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 10        | NULL            | **Yes**         |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 10        | NULL            | **Yes**         |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 10        | NULL            | **Yes**         |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 25        | NULL            | NULL            |

**Validation**:
- ✓ All 3 records with HC=10 (second-lowest) should have stage_2_masking = "Yes"
- ✓ Record with HC=6 should have stage_1_masking = "Yes"
- ✓ Record with HC=25 should remain unmasked

---

## Test Scenario 6: Multiple Metric IDs
**Purpose**: Verify masking logic is applied independently per metric_id

**Input Data**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 5         | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 12        | NULL            | NULL            |
| 2024-01-15  | metric_002 | Sales       | US-Region   | East        | US     | NYC      | 4         | NULL            | NULL            |
| 2024-01-15  | metric_002 | Sales       | US-Region   | East        | US     | NYC      | 8         | NULL            | NULL            |
| 2024-01-15  | metric_002 | Sales       | US-Region   | East        | US     | NYC      | 15        | NULL            | NULL            |

**Expected Output**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 5         | **Yes**         | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 12        | NULL            | **Yes**         |
| 2024-01-15  | metric_002 | Sales       | US-Region   | East        | US     | NYC      | 4         | **Yes**         | NULL            |
| 2024-01-15  | metric_002 | Sales       | US-Region   | East        | US     | NYC      | 8         | NULL            | **Yes**         |
| 2024-01-15  | metric_002 | Sales       | US-Region   | East        | US     | NYC      | 15        | NULL            | NULL            |

**Validation**:
- ✓ metric_001 group: Stage 1 on HC=5, Stage 2 on HC=12
- ✓ metric_002 group: Stage 1 on HC=4, Stage 2 on HC=8
- ✓ Metrics are treated as separate groups

---

## Test Scenario 7: Edge Case - Only Low Headcount Records
**Purpose**: Verify behavior when ALL records in a group have headcount < 7

**Input Data**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 2         | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 4         | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 6         | NULL            | NULL            |

**Expected Output**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 2         | **Yes**         | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 4         | **Yes**         | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 6         | **Yes**         | NULL            |

**Validation**:
- ✓ All records should have stage_1_masking = "Yes"
- ✓ NO records should have stage_2_masking = "Yes" (no records ≥ 7 exist)

---

## Test Scenario 8: Edge Case - No Low Headcount Records
**Purpose**: Verify behavior when NO records in a group have headcount < 7

**Input Data**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 10        | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 15        | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 25        | NULL            | NULL            |

**Expected Output**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 10        | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 15        | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 25        | NULL            | NULL            |

**Validation**:
- ✓ No records should be masked at Stage 1
- ✓ No records should be masked at Stage 2 (Stage 2 requires exactly 1 low HC)
- ✓ All masking columns remain NULL

---

## Test Scenario 9: Partition Isolation
**Purpose**: Verify that masking only affects the specified partition_dt

**Input Data**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 5         | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 12        | NULL            | NULL            |
| 2024-01-16  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 4         | NULL            | NULL            |
| 2024-01-16  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 15        | NULL            | NULL            |

**Run masking for partition_dt = '2024-01-15' only**

**Expected Output**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 5         | **Yes**         | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 12        | NULL            | **Yes**         |
| 2024-01-16  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 4         | NULL            | NULL            |
| 2024-01-16  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 15        | NULL            | NULL            |

**Validation**:
- ✓ Only partition_dt = '2024-01-15' records should be masked
- ✓ partition_dt = '2024-01-16' records should remain unchanged

---

## Test Scenario 10: Metric ID Filtering
**Purpose**: Verify that only specified metric_ids are processed

**Input Data**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 5         | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 12        | NULL            | NULL            |
| 2024-01-15  | metric_999 | Sales       | US-Region   | East        | US     | NYC      | 4         | NULL            | NULL            |
| 2024-01-15  | metric_999 | Sales       | US-Region   | East        | US     | NYC      | 15        | NULL            | NULL            |

**Run masking for metric_ids = ['metric_001'] only**

**Expected Output**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 5         | **Yes**         | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 12        | NULL            | **Yes**         |
| 2024-01-15  | metric_999 | Sales       | US-Region   | East        | US     | NYC      | 4         | NULL            | NULL            |
| 2024-01-15  | metric_999 | Sales       | US-Region   | East        | US     | NYC      | 15        | NULL            | NULL            |

**Validation**:
- ✓ Only metric_001 records should be masked
- ✓ metric_999 records should remain unchanged

---

## Test Scenario 11: Boundary Value - Headcount Exactly 7
**Purpose**: Verify correct behavior when headcount = 7 (boundary value)

**Input Data**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 6         | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 7         | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 8         | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 15        | NULL            | NULL            |

**Expected Output**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 6         | **Yes**         | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 7         | NULL            | **Yes**         |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 8         | NULL            | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 15        | NULL            | NULL            |

**Validation**:
- ✓ HC=6 (< 7) should have stage_1_masking = "Yes"
- ✓ HC=7 (≥ 7) should NOT have stage_1_masking
- ✓ HC=7 is the second-lowest ≥ 7, so should have stage_2_masking = "Yes"

---

## Test Scenario 12: Already Masked Records (Idempotency)
**Purpose**: Verify script handles records that are already masked (re-run scenario)

**Input Data**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 5         | Yes             | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 12        | NULL            | Yes             |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 20        | NULL            | NULL            |

**Expected Output**:
| partition_dt | metric_id  | org_level_1 | org_level_2 | org_level_3 | region | location | headcount | stage_1_masking | stage_2_masking |
|-------------|------------|-------------|-------------|-------------|--------|----------|-----------|-----------------|-----------------|
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 5         | Yes             | NULL            |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 12        | NULL            | Yes             |
| 2024-01-15  | metric_001 | Sales       | US-Region   | East        | US     | NYC      | 20        | NULL            | NULL            |

**Validation**:
- ✓ Already masked records should remain unchanged
- ✓ Script should be idempotent (safe to re-run)
- ✓ No duplicate masking or errors

---

## Validation Checklist

For each test scenario, verify:

1. **Stage 1 Correctness**:
   - All records with headcount < 7 have stage_1_masking = "Yes"
   - No records with headcount ≥ 7 have stage_1_masking = "Yes"

2. **Stage 2 Correctness**:
   - Stage 2 only applied to groups with exactly 1 low-headcount record
   - Stage 2 masks ALL records with second-lowest headcount value (≥ 7)
   - No records with headcount < 7 have stage_2_masking = "Yes"

3. **Grouping Correctness**:
   - Groups are defined by: metric_id + all hierarchy columns + all additional columns (region, location)
   - Different regions/locations create separate groups

4. **Isolation**:
   - Only specified partition_dt is affected
   - Only specified metric_ids are affected
   - Other records remain unchanged

5. **Data Integrity**:
   - No records are deleted
   - No records are inserted
   - Only stage_1_masking and stage_2_masking columns are modified
   - Values are only "Yes" or NULL (no other values)