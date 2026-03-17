import os
import json
import subprocess
import sys
import tempfile
import collections.abc

# ─── 1. COMPATIBILITY PATCH (For this script) ───────────────────────────
if not hasattr(collections, 'MutableMapping'):
    collections.MutableMapping = collections.abc.MutableMapping

# ─── 2. CONFIGURATION ─────────────────────────────────────────────────────
mysql_config = {
    "host": "localhost",
    "port": 3306,
    "user": "singer_admin",
    "password": "password123",
    "database": "singer_demonstration"
}

pg_config = {
    "host": "localhost",
    "port": 5432,
    "dbname": "singer_demo_db",
    "user": "postgres",
    "password": "Netspective",
    "schema": "mysql_to_postgres"
}

# ─── 3. SETUP TEMP FILES ──────────────────────────────────────────────────
TEMP_DIR = os.path.join(tempfile.gettempdir(), "singer_mysql_sync")
os.makedirs(TEMP_DIR, exist_ok=True)

tap_cfg_path = os.path.join(TEMP_DIR, "mysql_config.json")
target_cfg_path = os.path.join(TEMP_DIR, "postgres_config.json")
catalog_path = os.path.join(TEMP_DIR, "catalog.json")

with open(tap_cfg_path, "w") as f: json.dump(mysql_config, f, indent=2)
with open(target_cfg_path, "w") as f: json.dump(pg_config, f, indent=2)

def run_sync():
    # ─── 4. DISCOVERY ─────────────────────────────────────────────────────
    print("[►] Discovering MySQL Schemas...")
    result = subprocess.run(["tap-mysql", "--config", tap_cfg_path, "--discover"], capture_output=True, text=True)
    catalog = json.loads(result.stdout)

    # ─── 5. SANITIZE & SELECT ─────────────────────────────────────────────
    selected_count = 0
    new_streams = []
    for stream in catalog.get("streams", []):
        old_id = stream["tap_stream_id"]
        if old_id.startswith("singer_demonstration-"):
            clean_name = old_id.split("-")[-1].replace("-", "_")
            
            # Rename for the Target
            stream["stream"] = clean_name
            stream["tap_stream_id"] = clean_name
            stream["metadata"] = [{"breadcrumb": [], "metadata": {"selected": True, "replication-method": "FULL_TABLE"}}]
            
            new_streams.append(stream)
            selected_count += 1

    catalog["streams"] = new_streams
    with open(catalog_path, "w") as f:
        json.dump(catalog, f, indent=2)
    
    print(f"[✓] Catalog sanitized. Selected {selected_count} tables.")

    # ─── 6. EXECUTION WITH SUBPROCESS PATCH ───────────────────────────────
    print(f"[►] Syncing to Postgres (Applying Python 3.11 Patch to Target)...")
    
    # This command injects the MutableMapping fix into the target-postgres process
    patch_cmd = "import collections; import collections.abc; collections.MutableMapping = collections.abc.MutableMapping; from target_postgres import main; main()"

    try:
        tap_proc = subprocess.Popen(
            ["tap-mysql", "--config", tap_cfg_path, "--catalog", catalog_path], 
            stdout=subprocess.PIPE
        )
        
        # We run the target via 'python -c' to ensure the patch is applied before the logic runs
        target_proc = subprocess.run(
            [sys.executable, "-c", patch_cmd, "--config", target_cfg_path], 
            stdin=tap_proc.stdout, 
            capture_output=True, 
            text=True
        )
        
        if target_proc.returncode == 0:
            print("\n" + "="*40)
            print("[★] SUCCESS: Migration complete!")
            print(f"Data is now in Postgres schema: {pg_config['schema']}")
            print("="*40)
        else:
            print(f"\n[!] Sync Error in Target:\n{target_proc.stderr}")

    except Exception as e:
        print(f"[!] Runtime error: {e}")

if __name__ == "__main__":
    run_sync()