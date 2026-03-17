
### 📊 Singer.io CSV to PostgreSQL Sync

Here is your updated `README.md`. I have incorporated the latest fixes, including the permanent environment pathing and the integrated Python 3.11 patch that simplifies the file structure.

---

## 📊 Singer.io CSV to PostgreSQL Sync

An automated ingestion pipeline built with the **Singer.io** specification to move data from local CSV folders into a structured **PostgreSQL** schema.

### 🛠 Installation & Setup

#### 1. Environment Verification
Ensure your environment is up to date and check your versions:
```powershell
python -m pip install --upgrade pip
python --version
psql -U postgres -c "SELECT version();"
```

#### 2. Core Dependencies
Install the specific versions and forks required for this pipeline:
```powershell
# Core Singer libraries
python -m pip install singer-python colorama

# Specialized tap-csv fork (fixes some Windows pathing issues)
python -m pip install "git+https://github.com/hotgluexyz/tap-csv.git"

# Database drivers
python -m pip install psycopg2-binary psycopg2

# Target installation (using --no-deps to bypass compiler errors)
python -m pip install target-postgres --no-deps
python -m pip install inflection
```

---

### 🔧 Critical Fixes & Patches

#### 1. Fix `OverflowError` in `tap-csv`
On Windows, `sys.maxsize` exceeds C-limitations, causing the tap to crash. You must manually patch the library:
1.  **Locate the file:** `C:\Users\Anitha\AppData\Local\Programs\Python\Python311\Lib\site-packages\tap_csv\__init__.py`
2.  **Edit Line 17:** * *Find:* `csv.field_size_limit(sys.maxsize)`
    * *Change to:* `csv.field_size_limit(2147483647)`
3.  **Save and Close.**

#### 2. Environment Pathing (Command Not Recognized)
If `tap-csv` is not recognized, the Python Scripts folder is missing from your PATH. Run this in your session:
```powershell
$env:Path += ";C:\Users\Anitha\AppData\Local\Programs\Python\Python311\Scripts"
```
*(Note: The `demo_csv_to_postgres.py` script now includes an internal fix to handle this automatically during execution).*

#### 3. Python 3.11 `MutableMapping` Patch
`target-postgres` relies on legacy `collections` structures. The orchestrator script now includes a "monkey patch" to ensure compatibility with Python 3.11+:
```python
import collections.abc
collections.MutableMapping = collections.abc.MutableMapping
```

---

### 📂 Execution Files

1.  **`demo_csv_to_postgres.py`**: The main orchestrator. It handles environment pathing, the 3.11 compatibility patch, schema discovery, and data streaming.
2.  **`csv-test-files/`**: Directory containing your source `.csv` files (e.g., `customers.csv`).

---

### ⚡ Running the Sync

To execute the pipeline, provide the path to your CSV folder as an argument:

```powershell
python demo_csv_to_postgres.py "D:\Anitha_Varghese\singer-demo\csv-test-files"
```

### 🔍 Verification
After a successful run (`SUCCESS: Data pushed to Postgres`), verify the results in **pgAdmin**:
```sql
SELECT * FROM singer_data_ingest.customers;
SELECT * FROM singer_data_ingest.inventory;
SELECT * FROM singer_data_ingest.project_logs;
```

---

