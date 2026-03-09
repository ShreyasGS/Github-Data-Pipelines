# Module 3: Running dlt Inside Snowflake with SPCS — Demo Steps

## Naming Convention

| Item | Value |
|---|---|
| Script name | `github_api_pipeline.py` (unchanged) |
| `pipeline_name` | `github_api_spcs_pipeline` |
| `dataset_name` | `github_spcs_data` |
| Snowflake schema for data | `GITHUB_SPCS_DATA` |

---

## Pre-Demo Cleanup

### Snowflake (run in Snowsight)

```sql
USE ROLE ACCOUNTADMIN;

-- Drop existing job service from previous run
DROP SERVICE IF EXISTS DLT_DATA.GITHUB_DLT_SPCS.JOB_LOAD_GITHUB;

-- Drop scheduled task if it exists
DROP TASK IF EXISTS DLT_DATA.GITHUB_DLT_SPCS.TASK_LOAD_GITHUB;

-- Drop old data schemas for a clean demo
DROP SCHEMA IF EXISTS DLT_DATA.GITHUB_SPCS CASCADE;
DROP SCHEMA IF EXISTS DLT_DATA.GITHUB_SPCS_DATA CASCADE;
DROP SCHEMA IF EXISTS DLT_DATA.GITHUB_STG_API_DATA CASCADE;

-- Pre-warm compute pool (takes 1-2 min)
ALTER COMPUTE POOL CP_DLT_PIPELINE RESUME;
```

### Local terminal

```bash
cd ~/Desktop/SPCS_DEMO

# Remove local dlt pipeline state from previous runs
rm -rf github_api_spcs_pipeline/
rm -rf github_stg_api_pipeline/
```

---

## Step 1: Update Pipeline Script

In `github_api_pipeline.py`, change the pipeline config (around line 58-62):

**From:**
```python
pipeline = dlt.pipeline(
    pipeline_name="github_stg_api_pipeline",
    destination='snowflake',
    staging="filesystem",
    dataset_name="github_stg_api_data",
    progress="log",
)
```

**To:**
```python
pipeline = dlt.pipeline(
    pipeline_name="github_api_spcs_pipeline",
    destination='snowflake',
    staging="filesystem",
    dataset_name="github_spcs_data",
    progress="log",
)
```

---

## Step 2: Update config.toml

Add the staging truncation setting to `.dlt/config.toml`. Add this at the end:

```toml
[destination.snowflake]
truncate_tables_on_staging_destination_before_load = true
```

Full file should look like:

```toml
# put your configuration values here

[runtime]
log_level="WARNING"
dlthub_telemetry = true

[destination.snowflake]
truncate_tables_on_staging_destination_before_load = true
```

---

## Step 3: Verify secrets.toml

Confirm `.dlt/secrets.toml` has all credentials (should already be set from Module 2):

```toml
access_token = "ghp_your_github_token"

[destination.snowflake.credentials]
database = "DLT_DATA"
password = "your_password"
username = "SHREYASS"
host = "kgiotue-wn98412"
warehouse = "COMPUTE_WH"
role = "DLT_LOADER_ROLE"

[destination.filesystem.credentials]
aws_access_key_id = "YOUR_KEY"
aws_secret_access_key = "YOUR_SECRET"

[destination.filesystem]
bucket_url = "s3://gtm-demos/snowflake-demo/"
```

---

## Step 4: Test Pipeline Locally

```bash
python github_api_pipeline.py
```

Ensure it completes successfully before containerizing.

---

## Step 5: Create Dockerfile

Create a file named `Dockerfile` (no extension) in the project root:

```dockerfile
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY ./ /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-u", "github_api_pipeline.py"]
```

---

## Step 6: Create .dockerignore

Create `.dockerignore` in the project root:

```
__pycache__/
*.pyc
*.pyo
*.pyd
.Python
*.log
.git/
.gitignore
*.md
.DS_Store
.venv/
```

---

## Step 7: Create SPCS Service Specification

Create `load-github.yaml` in the project root:

```yaml
spec:
  containers:
    - name: load-github
      image: kgiotue-wn98412.registry.snowflakecomputing.com/dlt_data/github_dlt_spcs/image_repo/load_github:latest
```

---

## Step 8: Set Up Snowflake Infrastructure

Run in Snowflake (Snowsight):

### 8.1 Create Schema and Image Repository

```sql
USE ROLE ACCOUNTADMIN;

CREATE SCHEMA IF NOT EXISTS DLT_DATA.GITHUB_DLT_SPCS;

CREATE IMAGE REPOSITORY IF NOT EXISTS DLT_DATA.GITHUB_DLT_SPCS.IMAGE_REPO;

GRANT READ, WRITE ON IMAGE REPOSITORY DLT_DATA.GITHUB_DLT_SPCS.IMAGE_REPO
TO ROLE DLT_LOADER_ROLE;

SHOW IMAGE REPOSITORIES IN SCHEMA DLT_DATA.GITHUB_DLT_SPCS;
```

### 8.2 Create Stage for Service Specs

```sql
USE ROLE DLT_LOADER_ROLE;

CREATE STAGE IF NOT EXISTS DLT_DATA.GITHUB_DLT_SPCS.SPEC_STAGE
DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE);

SHOW STAGES IN SCHEMA DLT_DATA.GITHUB_DLT_SPCS;
```

### 8.3 Create Compute Pool

```sql
USE ROLE ACCOUNTADMIN;

CREATE COMPUTE POOL IF NOT EXISTS CP_DLT_PIPELINE
MIN_NODES = 1
MAX_NODES = 1
INSTANCE_FAMILY = CPU_X64_XS
AUTO_RESUME = TRUE
AUTO_SUSPEND_SECS = 60;

GRANT USAGE, MONITOR, OPERATE ON COMPUTE POOL CP_DLT_PIPELINE
TO ROLE DLT_LOADER_ROLE;

SHOW COMPUTE POOLS;
```

### 8.4 Create Network Rules and External Access

**Get Snowflake endpoints first:**

```sql
USE ROLE ACCOUNTADMIN;

SELECT LISTAGG(
    CONCAT('''', value:host::STRING, ':', value:port::NUMBER, ''''),
    ', '
)
FROM TABLE(FLATTEN(INPUT => PARSE_JSON(SYSTEM$ALLOWLIST())));
```

Copy the output, then create network rules:

```sql
USE ROLE ACCOUNTADMIN;

-- GitHub API
CREATE OR REPLACE NETWORK RULE DLT_DATA.GITHUB_DLT_SPCS.NR_GITHUB
MODE = 'EGRESS'
TYPE = 'HOST_PORT'
VALUE_LIST = ('api.github.com:443');

-- AWS S3
CREATE OR REPLACE NETWORK RULE DLT_DATA.GITHUB_DLT_SPCS.NR_S3
MODE = 'EGRESS'
TYPE = 'HOST_PORT'
VALUE_LIST = (
    's3.amazonaws.com:443',
    's3.eu-central-1.amazonaws.com:443',
    'gtm-demos.s3.amazonaws.com:443',
    'gtm-demos.s3.eu-central-1.amazonaws.com:443'
);

-- Snowflake (paste SYSTEM$ALLOWLIST() output into VALUE_LIST)
CREATE OR REPLACE NETWORK RULE DLT_DATA.GITHUB_DLT_SPCS.NR_SNOWFLAKE
MODE = 'EGRESS'
TYPE = 'HOST_PORT'
VALUE_LIST = (
    -- PASTE the comma-separated list from the query above here
);

-- dlt telemetry (optional)
CREATE OR REPLACE NETWORK RULE DLT_DATA.GITHUB_DLT_SPCS.NR_DLT_TELEMETRY
MODE = 'EGRESS'
TYPE = 'HOST_PORT'
VALUE_LIST = ('telemetry.scalevector.ai:443');
```

**Create External Access Integration:**

```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION EAI_GITHUB_DLT
ALLOWED_NETWORK_RULES = (
    DLT_DATA.GITHUB_DLT_SPCS.NR_GITHUB,
    DLT_DATA.GITHUB_DLT_SPCS.NR_S3,
    DLT_DATA.GITHUB_DLT_SPCS.NR_SNOWFLAKE,
    DLT_DATA.GITHUB_DLT_SPCS.NR_DLT_TELEMETRY
)
ENABLED = TRUE;

GRANT USAGE ON INTEGRATION EAI_GITHUB_DLT TO ROLE DLT_LOADER_ROLE;
```

---

## Step 9: Install Snowflake CLI

```bash
# Make sure virtual environment is activated
pip install snowflake-cli-labs

# Verify
snow --version
```

---

## Step 10: Configure Snowflake CLI Connection

```bash
mkdir -p ~/.snowflake

cat > ~/.snowflake/config.toml << 'EOF'
[connections.default]
account = "kgiotue-wn98412"
user = "SHREYASS"
password = "your_password_here"
role = "DLT_LOADER_ROLE"
warehouse = "COMPUTE_WH"
database = "DLT_DATA"
schema = "GITHUB_DLT_SPCS"
EOF

chmod 0600 ~/.snowflake/config.toml
```

**Important:** Replace the password with your actual password.

---

## Step 11: Test Snowflake CLI Connection

```bash
snow connection test
```

Expected output should show `Status: OK`.

---

## Step 12: Build and Push Docker Image

### 12.1 Make sure Docker Desktop is running

### 12.2 Build image

```bash
docker build --platform=linux/amd64 -t github-pipeline:latest .
```

### 12.3 Tag image for Snowflake registry

```bash
docker tag github-pipeline:latest \
  kgiotue-wn98412.registry.snowflakecomputing.com/dlt_data/github_dlt_spcs/image_repo/load_github:latest
```

### 12.4 Login to Snowflake registry

```bash
snow spcs image-registry login
```

### 12.5 Push image to Snowflake

```bash
docker push kgiotue-wn98412.registry.snowflakecomputing.com/dlt_data/github_dlt_spcs/image_repo/load_github:latest
```

### 12.6 Upload YAML spec to Snowflake stage

```bash
snow stage copy load-github.yaml @DLT_DATA.GITHUB_DLT_SPCS.SPEC_STAGE --overwrite
```

---

## Step 13: Run Pipeline in SPCS

```sql
USE ROLE DLT_LOADER_ROLE;
USE WAREHOUSE COMPUTE_WH;

EXECUTE JOB SERVICE
IN COMPUTE POOL CP_DLT_PIPELINE
NAME = DLT_DATA.GITHUB_DLT_SPCS.JOB_LOAD_GITHUB
EXTERNAL_ACCESS_INTEGRATIONS = (EAI_GITHUB_DLT)
FROM @DLT_DATA.GITHUB_DLT_SPCS.SPEC_STAGE
SPECIFICATION_FILE = 'load-github.yaml';
```

---

## Step 14: Monitor Job

Wait 1-2 minutes, then:

```sql
-- Check status
CALL SYSTEM$GET_JOB_STATUS('DLT_DATA.GITHUB_DLT_SPCS.JOB_LOAD_GITHUB');

-- View logs
CALL SYSTEM$GET_SERVICE_LOGS(
    'DLT_DATA.GITHUB_DLT_SPCS.JOB_LOAD_GITHUB',
    '0',
    'load-github',
    100
);
```

---

## Step 15: Verify Data

```sql
USE DATABASE DLT_DATA;
USE SCHEMA GITHUB_SPCS_DATA;

-- View all tables
SHOW TABLES IN SCHEMA GITHUB_SPCS_DATA;

-- Check repos
SELECT COUNT(*) AS total_repos FROM GITHUB_SPCS_DATA.REPOS;

SELECT
    NAME,
    FULL_NAME,
    STARGAZERS_COUNT,
    _DLT_LOAD_ID
FROM GITHUB_SPCS_DATA.REPOS
ORDER BY STARGAZERS_COUNT DESC
LIMIT 10;

-- Check issues
SELECT COUNT(*) AS total_issues FROM GITHUB_SPCS_DATA.ISSUES;

SELECT
    TITLE,
    STATE,
    UPDATED_AT,
    _DLT_LOAD_ID
FROM GITHUB_SPCS_DATA.ISSUES
ORDER BY UPDATED_AT DESC
LIMIT 10;

-- Check load history
SELECT
    LOAD_ID,
    SCHEMA_NAME,
    STATUS,
    INSERTED_AT
FROM GITHUB_SPCS_DATA._DLT_LOADS
ORDER BY INSERTED_AT DESC;
```

---

## Step 16: Re-Run to Test Reliability

```sql
USE ROLE DLT_LOADER_ROLE;

-- Must drop the existing job before re-running
DROP SERVICE DLT_DATA.GITHUB_DLT_SPCS.JOB_LOAD_GITHUB;

EXECUTE JOB SERVICE
IN COMPUTE POOL CP_DLT_PIPELINE
NAME = DLT_DATA.GITHUB_DLT_SPCS.JOB_LOAD_GITHUB
EXTERNAL_ACCESS_INTEGRATIONS = (EAI_GITHUB_DLT)
FROM @DLT_DATA.GITHUB_DLT_SPCS.SPEC_STAGE
SPECIFICATION_FILE = 'load-github.yaml';
```

Verify multiple loads:

```sql
SELECT
    LOAD_ID,
    SCHEMA_NAME,
    STATUS,
    INSERTED_AT
FROM GITHUB_SPCS_DATA._DLT_LOADS
ORDER BY INSERTED_AT DESC
LIMIT 5;
```

---

## Step 17: Schedule with Snowflake Task (Optional)

```sql
USE ROLE DLT_LOADER_ROLE;

CREATE OR REPLACE TASK DLT_DATA.GITHUB_DLT_SPCS.TASK_LOAD_GITHUB
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
EXECUTE JOB SERVICE
IN COMPUTE POOL CP_DLT_PIPELINE
NAME = DLT_DATA.GITHUB_DLT_SPCS.JOB_LOAD_GITHUB_SCHEDULED
EXTERNAL_ACCESS_INTEGRATIONS = (EAI_GITHUB_DLT)
FROM @DLT_DATA.GITHUB_DLT_SPCS.SPEC_STAGE
SPECIFICATION_FILE = 'load-github.yaml';

-- Activate the task
ALTER TASK DLT_DATA.GITHUB_DLT_SPCS.TASK_LOAD_GITHUB RESUME;

-- View task details
SHOW TASKS IN SCHEMA DLT_DATA.GITHUB_DLT_SPCS;
```

### Manage Task

```sql
-- Pause task
ALTER TASK DLT_DATA.GITHUB_DLT_SPCS.TASK_LOAD_GITHUB SUSPEND;

-- Resume task
ALTER TASK DLT_DATA.GITHUB_DLT_SPCS.TASK_LOAD_GITHUB RESUME;

-- View task run history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_LOAD_GITHUB',
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;
```

---

## Troubleshooting

| Issue | Fix |
|---|---|
| `docker build` fails | Is Docker Desktop running? Are you in the right directory? |
| `snow connection test` fails | Check account identifier, password (use single quotes for special chars) |
| Job fails with network error | Check logs. Likely missing S3 regional endpoint or GitHub endpoint in network rules |
| "Service already exists" error | Run `DROP SERVICE DLT_DATA.GITHUB_DLT_SPCS.JOB_LOAD_GITHUB;` first |
| Compute pool not starting | Run `ALTER COMPUTE POOL CP_DLT_PIPELINE RESUME;` and wait 1-2 min |
| Image push fails | Re-run `snow spcs image-registry login` to refresh auth |
