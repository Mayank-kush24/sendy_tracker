"""
Connection diagnostic script for Sendy Tracker.

Run this BEFORE starting Flask to verify your .env credentials are correct
and the Sendy tables are accessible.

Usage:
    python test_connection.py
"""

import sys
import os

# Load .env before importing db so credentials are available
from dotenv import load_dotenv
load_dotenv()

# ── Pretty helpers ─────────────────────────────────────────────────────────
def ok(msg):  print(f"  \033[32m✔\033[0m  {msg}")
def err(msg): print(f"  \033[31m✘\033[0m  {msg}")
def info(msg):print(f"  \033[34m→\033[0m  {msg}")
def hdr(msg): print(f"\n\033[1m{msg}\033[0m")

# ── Step 1: env vars ───────────────────────────────────────────────────────
hdr("1 / 4  Checking environment variables")
required = {
    "DB_HOST":     os.getenv("DB_HOST"),
    "DB_PORT":     os.getenv("DB_PORT"),
    "DB_NAME":     os.getenv("DB_NAME"),
    "DB_USER":     os.getenv("DB_USER"),
    "DB_PASSWORD": os.getenv("DB_PASSWORD"),
}
all_set = True
for key, val in required.items():
    if val:
        display = val if key != "DB_PASSWORD" else "*" * len(val)
        ok(f"{key} = {display}")
    else:
        err(f"{key} is NOT set – add it to your .env file")
        all_set = False

if not all_set:
    print("\n  Fix the missing values in .env and re-run.\n")
    sys.exit(1)

info(f"SSL  = {os.getenv('DB_SSL', 'false')}")
info(f"Timeout = {os.getenv('DB_CONNECT_TIMEOUT', '10')} s")

# ── Step 2: TCP reachability ───────────────────────────────────────────────
hdr("2 / 4  Testing TCP connection to MySQL port")
import socket
host = os.environ["DB_HOST"]
port = int(os.environ["DB_PORT"])
try:
    with socket.create_connection((host, port), timeout=10):
        ok(f"Port {port} is open on {host}")
except OSError as e:
    err(f"Cannot reach {host}:{port} — {e}")
    print()
    print("  Possible causes:")
    print("  • Your IP is not added in cPanel → Remote MySQL")
    print("  • The server firewall is blocking port 3306")
    print("  • DB_HOST or DB_PORT is wrong")
    print()
    sys.exit(1)

# ── Step 3: MySQL authentication ───────────────────────────────────────────
hdr("3 / 4  Authenticating with MySQL")
try:
    from db import get_engine
    from sqlalchemy import text
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("SELECT VERSION() AS v")).fetchone()
    ok(f"Connected!  MySQL version: {row.v}")
except Exception as e:
    err(f"Connection failed: {e}")
    print()
    print("  Possible causes:")
    print("  • Wrong DB_USER or DB_PASSWORD")
    print("  • DB_USER does not have access to DB_NAME")
    print("  • Try setting DB_SSL=true in .env if the server requires SSL")
    print()
    sys.exit(1)

# ── Step 4: Sendy tables ───────────────────────────────────────────────────
hdr("4 / 4  Checking Sendy tables")
sendy_tables = ["campaigns", "subscribers", "lists", "apps"]
from sqlalchemy import inspect as sa_inspect
insp = sa_inspect(engine)
found_tables = set(insp.get_table_names())

for table in sendy_tables:
    if table in found_tables:
        ok(f"Table '{table}' found")
    else:
        err(f"Table '{table}' NOT found in database '{os.environ['DB_NAME']}'")

# Show row counts for the key tables
hdr("  Row counts")
with engine.connect() as conn:
    for table in [t for t in sendy_tables if t in found_tables]:
        try:
            n = conn.execute(text(f"SELECT COUNT(*) AS n FROM `{table}`")).fetchone().n
            info(f"{table}: {n:,} rows")
        except Exception:
            pass

# Show campaign and subscriber column names (helps verify schema)
hdr("  campaigns columns")
with engine.connect() as conn:
    try:
        cols = conn.execute(text("DESCRIBE campaigns")).fetchall()
        for col in cols:
            info(f"  {col[0]:<30} {col[1]}")
    except Exception as e:
        err(f"Could not DESCRIBE campaigns: {e}")

hdr("  subscribers columns")
with engine.connect() as conn:
    try:
        cols = conn.execute(text("DESCRIBE subscribers")).fetchall()
        for col in cols:
            info(f"  {col[0]:<30} {col[1]}")
    except Exception as e:
        err(f"Could not DESCRIBE subscribers: {e}")

print()
print("  \033[32m\033[1mAll checks passed! You're ready to run Flask.\033[0m")
print()
print("  Start the app with:")
print("    flask --app app run --debug")
print()
