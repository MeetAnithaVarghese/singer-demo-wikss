#!/usr/bin/env python3
"""
Singer.io Demo 3: Write a Tap from Scratch (NO singer-python library)
=======================================================================
Run: python demo_custom_tap.py
  or: python demo_custom_tap.py 2>/dev/null          (raw Singer output only)
  or: python demo_custom_tap.py | python -m json.tool  (pretty-printed)

  Incremental (second run, only new rows):
    python demo_custom_tap.py 2>/dev/null | tail -1 > state.json
    python demo_custom_tap.py state.json 2>/dev/null

What this demo shows:
    ★ The Singer protocol is JUST 3 types of JSON printed to stdout ★
    No library required. No framework. No Singer SDK.
    If your script can print JSON — it's a valid Singer tap.

This is the most important demo. Understand this and you understand Singer.
"""

import json
import sys
from datetime import datetime

# ─── Our "data source" ────────────────────────────────────────────────────────
# Pretend this comes from a database query, REST API call, or file read.
INVENTORY = [
    {"id": 1, "product": "Widget Pro",        "qty": 100, "price": 9.99,   "updated_at": "2024-01-10T10:00:00Z"},
    {"id": 2, "product": "Gadget X",           "qty": 250, "price": 24.99,  "updated_at": "2024-02-15T14:30:00Z"},
    {"id": 3, "product": "Doohickey",          "qty":  50, "price": 4.99,   "updated_at": "2024-03-01T09:00:00Z"},
    {"id": 4, "product": "Thingamajig",        "qty":  75, "price": 14.99,  "updated_at": "2024-03-20T16:00:00Z"},
    {"id": 5, "product": "Super Nano Gizmo",   "qty": 320, "price": 39.99,  "updated_at": "2024-04-05T08:00:00Z"},
]

STREAM = "inventory"  # This becomes the table name at the target


# ─────────────────────────────────────────────────────────────────────────────
# THE ENTIRE SINGER PROTOCOL IS THIS FUNCTION:
# ─────────────────────────────────────────────────────────────────────────────
def emit(message: dict):
    """
    Singer protocol rule:
    - Print one JSON object per line to STDOUT
    - That's it. That's the protocol.
    """
    print(json.dumps(message), flush=True)


# ─── Message type 1: SCHEMA ───────────────────────────────────────────────────
def write_schema():
    """
    SCHEMA tells the target:
    - What stream (table) is coming
    - What fields exist and their types (JSON Schema format)
    - Which field(s) are the primary key (for upserts, not duplicates)

    This MUST be emitted BEFORE any RECORD messages for this stream.
    """
    emit({
        "type": "SCHEMA",
        "stream": STREAM,
        "schema": {
            "type": "object",
            "properties": {
                "id":         {"type": "integer",                      "description": "Primary key"},
                "product":    {"type": "string",                       "description": "Product name"},
                "qty":        {"type": "integer",                      "description": "Quantity in stock"},
                "price":      {"type": "number",                       "description": "Unit price"},
                "updated_at": {"type": "string", "format": "date-time","description": "Last updated timestamp"}
            },
            "required": ["id", "product"]
        },
        "key_properties": ["id"]  # ← PRIMARY KEY — target uses this for upserts
    })


# ─── Message type 2: RECORD ───────────────────────────────────────────────────
def write_records(last_bookmark: str = None):
    """
    RECORD carries the actual data. One JSON object per row.

    For incremental sync: only emit rows where updated_at > last_bookmark.
    This is how Singer avoids re-syncing data you've already loaded.
    """
    emitted = 0
    skipped = 0

    for row in INVENTORY:
        # INCREMENTAL FILTER: skip rows we've already synced
        if last_bookmark and row["updated_at"] <= last_bookmark:
            skipped += 1
            print(f"# SKIP: {row['product']} (updated_at={row['updated_at']} <= bookmark={last_bookmark})", file=sys.stderr)
            continue

        emit({
            "type": "RECORD",
            "stream": STREAM,
            "record": row  # ← must match keys defined in SCHEMA above
        })
        emitted += 1

    print(f"# Emitted {emitted} records, skipped {skipped} (already synced)", file=sys.stderr)
    return emitted


# ─── Message type 3: STATE ────────────────────────────────────────────────────
def write_state():
    """
    STATE is the bookmark. It tells the next run where to resume from.

    The TARGET writes this value to a state file after a successful run.
    The next tap invocation receives it via --state flag and resumes.

    Without STATE, every run would be a full reload.
    With STATE, only new/changed rows are synced — this is incremental sync.
    """
    latest_updated_at = max(row["updated_at"] for row in INVENTORY)

    emit({
        "type": "STATE",
        "value": {
            STREAM: {
                "updated_at": latest_updated_at,        # ← resume from here next run
                "synced_at":  datetime.utcnow().isoformat() + "Z",
                "record_count": len(INVENTORY)
            }
        }
    })


# ─────────────────────────────────────────────────────────────────────────────
# MAIN: Emit in the correct Singer order
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":

    # ── Load existing state (for incremental runs) ────────────────────────────
    state = {}
    last_bookmark = None

    if len(sys.argv) > 1:
        state_file = sys.argv[1]
        try:
            with open(state_file) as f:
                state = json.load(f)
            last_bookmark = state.get("value", {}).get(STREAM, {}).get("updated_at")
            print(f"# Incremental run — resuming from bookmark: {last_bookmark}", file=sys.stderr)
        except FileNotFoundError:
            print(f"# State file not found: {state_file} — doing full sync", file=sys.stderr)
    else:
        print("# First run — full sync (no state file provided)", file=sys.stderr)

    print(f"# Stream: {STREAM}  Source: {len(INVENTORY)} rows in INVENTORY list", file=sys.stderr)
    print("#", file=sys.stderr)

    # ── Singer protocol order: SCHEMA → RECORDs → STATE ──────────────────────
    # 1. SCHEMA first — target needs to know the structure before any data
    write_schema()

    # 2. RECORDs — emit only rows newer than last bookmark (or all if first run)
    record_count = write_records(last_bookmark)

    # 3. STATE last — bookmark for next run
    write_state()

    print(f"# Done. Singer stream complete: 1 SCHEMA, {record_count} RECORDs, 1 STATE", file=sys.stderr)


"""
═══════════════════════════════════════════════════════════════════════════
USAGE EXAMPLES — run these in your terminal
═══════════════════════════════════════════════════════════════════════════

1. See the raw Singer stream:
   python demo_custom_tap.py 2>/dev/null

2. Pretty-print it:
   python demo_custom_tap.py 2>/dev/null | python -m json.tool

3. Count message types:
   python demo_custom_tap.py 2>/dev/null | python3 -c "
   import sys, json, collections
   counts = collections.Counter()
   for line in sys.stdin:
       msg = json.loads(line.strip())
       counts[msg['type']] += 1
   print(dict(counts))"

4. Demonstrate incremental sync:
   # First run — sync all rows, save state:
   python demo_custom_tap.py 2>/dev/null | tee full_output.jsonl | tail -1 > state.json
   cat state.json

   # Second run — only rows NEWER than bookmark (should be 0 new rows!):
   python demo_custom_tap.py state.json 2>/dev/null

5. Pipe to a real Singer target:
   pip install target-csv
   python demo_custom_tap.py 2>/dev/null | target-csv --config '{"delimiter":","}'
   # Produces: inventory.csv in current directory

6. Pipe to target-postgres:
   pip install target-postgres
   python demo_custom_tap.py 2>/dev/null \\
     | target-postgres --config target_pg_config.json
   # Creates 'inventory' table with 5 rows

═══════════════════════════════════════════════════════════════════════════
KEY INSIGHT: This entire tap is ~40 lines of business logic.
The "Singer integration" is 1 function: emit() = json.dumps() to stdout.
═══════════════════════════════════════════════════════════════════════════
"""
