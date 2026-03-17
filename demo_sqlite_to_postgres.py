#!/usr/bin/env python3
"""
Singer.io Demo 1: SQLite → PostgreSQL
======================================
Run: python demo_sqlite_to_postgres.py

Requires:
    pip install tap-sqlite target-postgres

What this demo shows:
    1. Creates a SQLite DB with a sample 'orders' table
    2. Runs tap-sqlite in --discover mode to list available streams
    3. Prints the raw Singer JSON stream (SCHEMA → RECORD → STATE)
    4. Pipes the stream to target-postgres (if Postgres is available)

The key insight: the tap just prints JSON to stdout.
That's the entire Singer protocol for the tap side.
"""

import sqlite3
import subprocess
import json
import os
import sys

COLORS = {
    "green":  "\033[92m",
    "blue":   "\033[94m",
    "cyan":   "\033[96m",
    "yellow": "\033[93m",
    "red":    "\033[91m",
    "reset":  "\033[0m",
    "bold":   "\033[1m",
}

def p(color, msg):
    print(f"{COLORS[color]}{msg}{COLORS['reset']}")

# ─── Step 1: Create sample SQLite database ────────────────────────────────────
p("bold", "\n╔══════════════════════════════════════════════════╗")
p("bold", "║  Singer.io Demo 1: SQLite → PostgreSQL           ║")
p("bold", "╚══════════════════════════════════════════════════╝")

db_path = "/tmp/demo_orders.db"

# Remove if exists to start fresh
if os.path.exists(db_path):
    os.remove(db_path)

conn = sqlite3.connect(db_path)
conn.executescript("""
    CREATE TABLE IF NOT EXISTS orders (
        id          INTEGER PRIMARY KEY,
        customer    TEXT    NOT NULL,
        amount      REAL    NOT NULL,
        status      TEXT    DEFAULT 'pending',
        created_at  TEXT    DEFAULT (datetime('now'))
    );
    INSERT INTO orders (id, customer, amount, status) VALUES
        (1, 'Alice Corp',    1250.00, 'completed'),
        (2, 'Bob Ltd',        890.50, 'pending'),
        (3, 'Carol GmbH',    3400.00, 'completed'),
        (4, 'Dave Inc',       150.75, 'cancelled'),
        (5, 'Eve Solutions', 2100.00, 'pending');
""")
conn.commit()
conn.close()
p("green", f"\n[✓] Step 1: SQLite DB created at {db_path}")
p("cyan",  "    Table: orders (5 rows)")

# ─── Step 2: Write tap config ─────────────────────────────────────────────────
tap_config = {
    "database": db_path,
    "start_date": "2020-01-01T00:00:00Z"
}
tap_cfg_file = "/tmp/singer_tap_sqlite_config.json"
with open(tap_cfg_file, "w") as f:
    json.dump(tap_config, f, indent=2)
p("green", f"\n[✓] Step 2: Tap config written to {tap_cfg_file}")
p("cyan",  f"    {json.dumps(tap_config, indent=2)}")

# ─── Step 3: Run discovery ────────────────────────────────────────────────────
p("yellow", "\n[►] Step 3: Running tap-sqlite --discover ...")
try:
    discover = subprocess.run(
        ["tap-sqlite", "--config", tap_cfg_file, "--discover"],
        capture_output=True, text=True, timeout=30
    )
    if discover.returncode != 0:
        p("red", f"[!] tap-sqlite discover failed: {discover.stderr}")
        p("red", "    Make sure tap-sqlite is installed: pip install tap-sqlite")
        sys.exit(1)

    catalog = json.loads(discover.stdout)
    p("green", f"[✓] Discovered {len(catalog['streams'])} stream(s):")
    for stream in catalog["streams"]:
        p("cyan", f"    → {stream['tap_stream_id']}")
except FileNotFoundError:
    p("red", "[!] tap-sqlite not found. Install with: pip install tap-sqlite")
    sys.exit(1)

# ─── Step 4: Prepare catalog — select 'orders' stream ────────────────────────
catalog_file = "/tmp/singer_catalog.json"
for stream in catalog["streams"]:
    if stream["tap_stream_id"] == "orders":
        # Clear existing metadata and set our selection
        stream["metadata"] = [{
            "breadcrumb": [],
            "metadata": {
                "selected": True,
                "replication-method": "FULL_TABLE"
            }
        }]
with open(catalog_file, "w") as f:
    json.dump(catalog, f)
p("green", f"\n[✓] Step 4: Catalog prepared — 'orders' stream selected")

# ─── Step 5: Run tap and display raw Singer stream ────────────────────────────
p("yellow", "\n[►] Step 5: Running tap-sqlite — raw Singer JSON stream:\n")
p("bold", "─" * 70)

tap_out = subprocess.run(
    ["tap-sqlite", "--config", tap_cfg_file, "--catalog", catalog_file],
    capture_output=True, text=True, timeout=60
)

schema_count = record_count = state_count = 0
for line in tap_out.stdout.strip().split("\n"):
    if not line.strip():
        continue
    try:
        msg = json.loads(line)
        msg_type = msg.get("type", "?")
        if msg_type == "SCHEMA":
            schema_count += 1
            props = list(msg["schema"]["properties"].keys())
            p("blue", f"  ┌─ SCHEMA  stream={msg['stream']}")
            p("blue", f"  │  fields: {props}")
            p("blue", f"  │  key_properties: {msg.get('key_properties', [])}")
            p("blue",  "  └─")
        elif msg_type == "RECORD":
            record_count += 1
            p("cyan", f"  RECORD  {msg['record']}")
        elif msg_type == "STATE":
            state_count += 1
            p("yellow", f"  STATE   {msg['value']}")
    except json.JSONDecodeError:
        print(f"  [raw] {line}")

p("bold", "─" * 70)
p("green", f"\n[✓] Singer stream summary:")
p("cyan",  f"    SCHEMA  messages: {schema_count}")
p("cyan",  f"    RECORD  messages: {record_count}")
p("cyan",  f"    STATE   messages: {state_count}")

# ─── Step 6: Pipe to target-postgres ─────────────────────────────────────────
p("yellow", "\n[►] Step 6: Piping to target-postgres ...")
p("cyan",   "    (Edit pg_config below with your Postgres credentials)")

pg_config = {
    "host":                   "localhost",
    "port":                   5432,
    "dbname":                 "demo_db",
    "user":                   "postgres",
    "password":               "postgres",
    "default_target_schema":  "public"
}
pg_cfg_file = "/tmp/singer_target_pg_config.json"
with open(pg_cfg_file, "w") as f:
    json.dump(pg_config, f, indent=2)

try:
    tap_p = subprocess.Popen(
        ["tap-sqlite", "--config", tap_cfg_file, "--catalog", catalog_file],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
    )
    target_p = subprocess.Popen(
        ["target-postgres", "--config", pg_cfg_file],
        stdin=tap_p.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    tap_p.stdout.close()
    out, err = target_p.communicate(timeout=60)

    if target_p.returncode == 0:
        p("green", "[✓] Pipeline complete!")
        p("cyan",  "    Check table 'public.orders' in your Postgres database.")
        p("cyan",  "    Run: psql -c 'SELECT * FROM orders;'")
    else:
        p("yellow", "[!] target-postgres returned an error:")
        p("yellow", f"    {err.decode()[:300]}")
        p("cyan",   "    This is normal if Postgres isn't running locally.")
        p("cyan",   "    The Singer stream output above shows the full protocol.")
except FileNotFoundError:
    p("yellow", "[!] target-postgres not found.")
    p("cyan",   "    Install: pip install target-postgres")
    p("cyan",   "    The Singer stream output above is the complete protocol demonstration.")
except Exception as e:
    p("yellow", f"[!] Could not reach Postgres: {e}")
    p("cyan",   "    The Singer stream output above demonstrates the full protocol.")

p("bold", "\n╔══════════════════════════════════════════════════════════════╗")
p("bold", "║  COMPLETE PIPELINE COMMAND (one line):                       ║")
p("bold", "║                                                               ║")
p("bold", "║  tap-sqlite --config tap.json --catalog catalog.json  \\      ║")
p("bold", "║    | target-postgres --config pg.json                         ║")
p("bold", "╚══════════════════════════════════════════════════════════════╝")
