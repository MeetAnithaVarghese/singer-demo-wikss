#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import glob
import tempfile
import platform

# Initialize Colorama
try:
    from colorama import init, Fore, Style
    init()
    COLORS = {"green": Fore.GREEN, "cyan": Fore.CYAN, "yellow": Fore.YELLOW, "red": Fore.RED, "reset": Style.RESET_ALL, "bold": Style.BRIGHT}
except ImportError:
    COLORS = {k: "" for k in ["green", "cyan", "yellow", "red", "reset", "bold"]}

def p(color, msg):
    print(f"{COLORS.get(color, '')}{msg}{COLORS['reset']}")

# ─── CONFIGURATION ───────────────────────────────────────────────────────────
CSV_INPUT_FOLDER = sys.argv[1] if len(sys.argv) > 1 else r"D:\Anitha_Varghese\singer-demo\csv-test-files"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "singer_demo_db",
    "user": "postgres",
    "password": "Netspective", 
    "schema": "singer_data_ingest"
}

TEMP_DIR = os.path.join(tempfile.gettempdir(), "singer_ops")
os.makedirs(TEMP_DIR, exist_ok=True)

p("bold", "\n╔══════════════════════════════════════════════════════════════╗")
p("bold", "║    Singer.io: Automated CSV Folder → PostgreSQL Sync         ║")
p("bold", "╚══════════════════════════════════════════════════════════════╝")

# ─── Step 1: Scan Folder ───────────────────────────────────────────────────
p("yellow", f"\n[►] Step 1: Scanning {CSV_INPUT_FOLDER} for files...")
csv_files = glob.glob(os.path.join(CSV_INPUT_FOLDER, "*.csv"))

files_config = []
for f_path in csv_files:
    entity = os.path.splitext(os.path.basename(f_path))[0]
    # Use absolute paths with forward slashes
    safe_path = os.path.abspath(f_path).replace("\\", "/")
    
    files_config.append({
        "entity": entity, 
        "path": safe_path, 
        "file": safe_path,  # Added for compatibility
        "keys": []
    })
    p("green", f"    [✓] Found: {entity}")

# ─── Step 2: Write Configs ──────────────────────────────────────────────────
tap_cfg_path = os.path.join(TEMP_DIR, "tap_config.json").replace("\\", "/")
target_cfg_path = os.path.join(TEMP_DIR, "target_config.json").replace("\\", "/")

with open(tap_cfg_path, "w") as f: json.dump({"files": files_config}, f)
with open(target_cfg_path, "w") as f: json.dump(DB_CONFIG, f)

# ─── Step 3: Discover ──────────────────────────────────────────────────────
# ─── Step 3: Discover (Updated for Debugging) ──────────────────────────────
p("yellow", "\n[►] Step 3: Discovering schemas...")
catalog_path = os.path.join(TEMP_DIR, "catalog.json").replace("\\", "/")

try:
    proc_cmd = f'tap-csv --config "{tap_cfg_path}" --discover'
    # Adding capture_output=True lets us see the real error message
    disc = subprocess.run(proc_cmd, capture_output=True, text=True, check=True, shell=True)
    
    catalog = json.loads(disc.stdout)
    for stream in catalog.get("streams", []):
        stream["metadata"] = [{"breadcrumb": [], "metadata": {"selected": True, "replication-method": "FULL_TABLE"}}]
    
    with open(catalog_path, "w") as f: json.dump(catalog, f)
    p("green", f"    [✓] Schema inference complete. {len(catalog.get('streams', []))} tables mapped.")

except subprocess.CalledProcessError as e:
    p("red", "\n[!] THE TOOL CRASHED. REAL ERROR BELOW:")
    print(e.stderr)  # THIS IS THE IMPORTANT LINE
    sys.exit(1)
except Exception as e:
    p("red", f"    [!] Unexpected error: {str(e)}")
    sys.exit(1)

# ─── Step 4: Execution ──────────────────────────────────────────────────────
p("yellow", "\n[►] Step 4: Executing data stream...")

# We call the patch_target.py script we just created
sync_cmd = (
    f'tap-csv --config "{tap_cfg_path}" --catalog "{catalog_path}" | '
    f'python patch_target.py --config "{target_cfg_path}"'
)

try:
    p("cyan", "    Stream started with Python 3.11 compatibility patch...")
    subprocess.run(sync_cmd, shell=True, check=True)
    p("green", "  SUCCESS: Data pushed to Postgres.")
except subprocess.CalledProcessError as e:
    p("red", f"\n[!] Sync failed with exit code {e.returncode}.")