# dlt Staging with Snowflake - Learning Report

## Overview

When using dlt to load data from APIs to Snowflake, there are **two types of staging** that can be used:

1. **S3 Staging (Filesystem)** - External cloud storage for staging files
2. **Snowflake Staging Dataset** - Internal database schema for temporary tables

---

## 1. S3 Staging (Filesystem)

### What is it?
Files are uploaded to an S3 bucket before Snowflake loads them using `COPY INTO` commands.

### When is it used?
- **Always** when you configure `staging="filesystem"` in the pipeline
- Works with **all write dispositions**: `append`, `replace`, `merge`

### Configuration
```toml
# .dlt/secrets.toml
[destination.filesystem.credentials]
aws_access_key_id = "your_key"
aws_secret_access_key = "your_secret"

[destination.filesystem]
bucket_url = "s3://your-bucket/path/"
```

### Cleanup Control

#### Option 1: Truncate staging files before each load (default: true)
```toml
# .dlt/config.toml
[destination.snowflake]
truncate_tables_on_staging_destination_before_load = true  # Default
```

**Behavior:**
- `true`: Old staging files are deleted before new load → Only latest files remain
- `false`: Staging files accumulate across runs → Full history preserved

**Test command:**
```bash
python view_s3_staging.py  # View S3 staging files
```

---

## 2. Snowflake Staging Dataset

### What is it?
A separate schema in Snowflake (e.g., `<dataset_name>_staging`) containing temporary staging tables.

### When is it created?

| Write Disposition | Strategy | Creates Staging Dataset? |
|-------------------|----------|-------------------------|
| `append` | (default) | ❌ No |
| `replace` | `truncate-and-insert` (default) | ❌ No |
| `replace` | `insert-from-staging` | ✅ Yes |
| `replace` | `staging-optimized` | ✅ Yes |
| `merge` | `delete-insert` (default) | ✅ Yes |
| `merge` | `scd2` | ✅ Yes |
| `merge` | `upsert` | ✅ Yes |

### Configuration for Merge

```python
config: RESTAPIConfig = {
    "resources": [
        {
            "name": "my_resource",
            "write_disposition": "merge",  # Creates staging dataset
            "primary_key": "id",           # Required for merge
            "endpoint": {
                "path": "api/endpoint"
            },
        },
    ],
}
```

### Cleanup Control

#### Option: Truncate staging dataset after load (default: false)
```toml
# .dlt/config.toml
[load]
truncate_staging_dataset = true  # Clean up after load
```

**Behavior:**
- `false` (default): Staging tables remain populated after load (useful for debugging)
- `true`: Staging tables are truncated after successful load → Empty tables

**Test in Snowflake:**
```sql
-- Check staging dataset exists
SHOW SCHEMAS LIKE '%_staging';

-- Query staging table
SELECT * FROM <dataset_name>_staging.<table_name>;
```

---

## Write Disposition Summary

### append (default)
- **S3 Staging**: ✅ Used (if `staging="filesystem"`)
- **Snowflake Staging Dataset**: ❌ Not created
- **Flow**: S3 → Direct `COPY INTO` → Final tables

### replace

#### Strategy: `truncate-and-insert` (default)
- **S3 Staging**: ✅ Used (if `staging="filesystem"`)
- **Snowflake Staging Dataset**: ❌ Not created
- **Flow**: S3 → `COPY INTO` → Truncate & replace final tables

#### Strategy: `insert-from-staging`
- **S3 Staging**: ✅ Used (if `staging="filesystem"`)
- **Snowflake Staging Dataset**: ✅ Created
- **Flow**: 
  ```
  S3 files
    ↓ (COPY INTO)
  Snowflake staging tables
    ↓ (INSERT)
  Truncate & insert into final tables
  ```

#### Strategy: `staging-optimized`
- **S3 Staging**: ✅ Used (if `staging="filesystem"`)
- **Snowflake Staging Dataset**: ✅ Created
- **Flow**: 
  ```
  S3 files
    ↓ (COPY INTO)
  Snowflake staging tables
    ↓ (CLONE optimization)
  Final tables dropped & recreated from staging
  ```

### merge

All merge strategies create Snowflake staging dataset.

#### Strategy: `delete-insert` (default)
- **S3 Staging**: ✅ Used (if `staging="filesystem"`)
- **Snowflake Staging Dataset**: ✅ Created
- **Primary/Merge Key**: Required
- **Flow**: 
  ```
  S3 files
    ↓ (COPY INTO)
  Snowflake staging tables
    ↓ (DELETE matching records, then INSERT)
  Final tables
  ```
- **Configuration:**
  ```python
  {
      "name": "my_resource",
      "write_disposition": "merge",  # Uses delete-insert by default
      "primary_key": "id",
  }
  ```

#### Strategy: `scd2` (Slowly Changing Dimension Type 2)
- **S3 Staging**: ✅ Used (if `staging="filesystem"`)
- **Snowflake Staging Dataset**: ✅ Created
- **Primary/Merge Key**: Optional (for incremental mode)
- **Tracks History**: Adds `_dlt_valid_from` and `_dlt_valid_to` columns
- **Flow**: 
  ```
  S3 files
    ↓ (COPY INTO)
  Snowflake staging tables
    ↓ (Compare row hashes, update validity timestamps)
  Final tables (with historical records)
  ```
- **Configuration:**
  ```python
  {
      "name": "my_resource",
      "write_disposition": {
          "disposition": "merge",
          "strategy": "scd2"
      },
      # Optional: for incremental loading
      "merge_key": "natural_key",
  }
  ```

#### Strategy: `upsert`
- **S3 Staging**: ✅ Used (if `staging="filesystem"`)
- **Snowflake Staging Dataset**: ✅ Created
- **Primary Key**: Required (must be unique)
- **Flow**: 
  ```
  S3 files
    ↓ (COPY INTO)
  Snowflake staging tables
    ↓ (MERGE: UPDATE existing, INSERT new)
  Final tables
  ```
- **Configuration:**
  ```python
  {
      "name": "my_resource",
      "write_disposition": {
          "disposition": "merge",
          "strategy": "upsert"
      },
      "primary_key": "id",  # Must be unique
  }
  ```

---

## Merge Strategies Comparison

| Strategy | Use Case | Requires PK/MK | Tracks History | Deduplication |
|----------|----------|----------------|----------------|---------------|
| `delete-insert` | Keep latest version of each record | Yes (primary_key or merge_key) | No | Yes (in staging) |
| `scd2` | Track all changes over time | Optional (merge_key for incremental) | Yes (validity columns) | N/A |
| `upsert` | Update or insert based on unique key | Yes (unique primary_key) | No | No |

### When to use each merge strategy:

**delete-insert (default):**
- You receive daily batches and want only one instance per record
- You're syncing API data and want the latest state
- Example: User profiles, product catalog

**scd2:**
- You need to maintain historical versions of records
- Audit trail is important
- Example: Customer addresses over time, price changes

**upsert:**
- Your source guarantees unique primary keys
- You want efficient updates (uses native MERGE command)
- Example: Transaction records with unique IDs

---

## Complete Configuration Example

### For merge with both S3 and Snowflake staging:

```python
# pipeline_with_merge.py
import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source

config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.github.com",
        "auth": {
            "token": dlt.secrets["sources.access_token"],
        },
    },
    "resources": [
        {
            "name": "repos_merge_test",
            "write_disposition": "merge",
            "primary_key": "id",
            "endpoint": {
                "path": "orgs/dlt-hub/repos"
            },
        },
    ],
}

pipeline = dlt.pipeline(
    pipeline_name="github_merge_test",
    destination="snowflake",
    staging="filesystem",  # Enables S3 staging
    dataset_name="github_snw_merge_test",
)

pipeline.run(rest_api_source(config))
```

```toml
# .dlt/config.toml
[load]
# Truncate Snowflake staging dataset after load
truncate_staging_dataset = true

[destination.snowflake]
# Truncate S3 staging files before load
truncate_tables_on_staging_destination_before_load = true
```

---

## Key Commands Used

### View S3 staging files
```bash
python view_s3_staging.py
```

### Clean S3 staging manually
```bash
python cleanup_s3_staging.py
```

### Run pipeline
```bash
python pipeline_with_staging.py  # append mode
python pipeline_with_merge.py     # merge mode
```

### Check Snowflake schemas
```sql
SHOW SCHEMAS;  -- Look for <dataset>_staging
```

---

## Configuration Files Structure

```
.dlt/
├── config.toml           # Non-sensitive configuration
│   ├── [load]
│   │   └── truncate_staging_dataset
│   └── [destination.snowflake]
│       └── truncate_tables_on_staging_destination_before_load
│
└── secrets.toml          # Sensitive credentials
    ├── [destination.snowflake.credentials]
    │   ├── database, username, password, etc.
    └── [destination.filesystem.credentials]
        ├── aws_access_key_id
        └── aws_secret_access_key
```

---

## Testing Staging Behavior

### Test 1: S3 Staging Truncation
```bash
# Run 1
python pipeline_with_staging.py
python view_s3_staging.py  # Note load_id in filenames

# Run 2
python pipeline_with_staging.py
python view_s3_staging.py  # Old load_id gone, new load_id present
```

### Test 2: Snowflake Staging Dataset Cleanup
```bash
# Set truncate_staging_dataset = false
python pipeline_with_merge.py

# Check Snowflake - staging tables have data
SELECT * FROM github_snw_merge_test_staging.repos_merge_test;

# Set truncate_staging_dataset = true
python pipeline_with_merge.py

# Check Snowflake - staging tables are empty
SELECT * FROM github_snw_merge_test_staging.repos_merge_test;
```

---

## Best Practices

1. **For production:**
   - Set `truncate_tables_on_staging_destination_before_load = true` (keep only latest S3 files)
   - Set `truncate_staging_dataset = true` (clean up Snowflake staging after load)

2. **For debugging:**
   - Set both to `false` to preserve staging data for inspection

3. **Cost optimization:**
   - Clean up S3 files regularly (they accumulate and cost storage)
   - Use S3 lifecycle policies as an additional safeguard

4. **Write disposition choice:**
   - Use `append` for immutable event data
   - Use `merge` for data that can change (requires `primary_key`)
   - Use `replace` for full refresh scenarios

---

## Summary Table

| Setting | Location | Default | Effect |
|---------|----------|---------|--------|
| `truncate_tables_on_staging_destination_before_load` | `[destination.snowflake]` | `true` | Truncates **S3 files** before load |
| `truncate_staging_dataset` | `[load]` | `false` | Truncates **Snowflake staging tables** after load |
| `staging="filesystem"` | `dlt.pipeline()` | None | Enables **S3 staging** |
| `write_disposition="merge"` | Resource config | `append` | Creates **Snowflake staging dataset** |

---

## Verified Behavior

✅ **S3 Staging**: Files are truncated BEFORE each load when `truncate_tables_on_staging_destination_before_load = true`

✅ **Snowflake Staging Dataset**: Tables are truncated AFTER successful load when `truncate_staging_dataset = true`

✅ **Double Staging**: Both S3 and Snowflake staging work together for `merge` write disposition

✅ **Write Disposition Control**: Can be set per-resource in the REST API config
