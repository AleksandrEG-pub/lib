
## Table Descriptions

### 'raw_employees'
This is the initial staging area where data first arrives from source systems.
Raw, append only, exactly as received from source

| Column | Type | Description         |
|--------|------|---------------------|
| `employee_id` | BIGINT | Unique identifier for each employee from the source system |
| `name` | VARCHAR(100) | Employee's name     |
| `age` | VARCHAR(5) | Age stored as text  |
| `city` | VARCHAR(100) | Employee's city of residence|
| `salary` | VARCHAR(15) | Salary stored as text with possible formatting issues |
| `loaded_at` | TIMESTAMP | Audit column. System timestamp when record was ingested|
| `record_source` | VARCHAR(100) | Audit column. Identifies the source system/file |

---

### 'stage_employees'
Clean, typed, deduplicated data

| Column | Type | Description                                                 |
|--------|------|-------------------------------------------------------------|
| `hash_key` | VARCHAR(100) | PRIMARY KEY - Unique hash, md5(name, age, city, salary) |
| `employee_id` | BIGINT | Employee ID now properly typed as integer                   |
| `name` | VARCHAR(100) | Employee name (could be trimmed/cleaned)                    |
| `age` | INTEGER | Age properly cast to integer (rejecting invalid values)     |
| `city` | VARCHAR(100) | City name (may be standardized)                             |
| `salary` | DECIMAL(10,2) | Salary as precise decimal value for calculations            |
| `loaded_at` | TIMESTAMP | Original load timestamp from raw table                      |
| `record_source` | VARCHAR(100) | Original source identifier carried forward                  |

---

### 'data_quality_checks'
Tracks all data quality validations performed during processing.

| Column | Type | Description                                                 |
|--------|------|-------------------------------------------------------------|
| `check_id` | BIGSERIAL | Auto-incrementing unique identifier for each quality check  |
| `check_name` | VARCHAR(50) | Name of the validation (e.g., "fullness", "freshness")      |
| `status` | VARCHAR(50) | Result of check, depends on check type                  |
| `value` | VARCHAR(100) | Additional metric, depends on check type                    |
| `timestamp` | TIMESTAMP | When the check was executed (defaults to current timestamp) |
