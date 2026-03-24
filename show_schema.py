"""
Prints every table in the connected database with its columns,
types, keys, and a live row count.

Usage:
    python show_schema.py
"""

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import text
from db import get_engine

# ── helpers ────────────────────────────────────────────────────────────────
RESET  = "\033[0m"
BOLD   = "\033[1m"
CYAN   = "\033[36m"
YELLOW = "\033[33m"
GREEN  = "\033[32m"
DIM    = "\033[2m"

def hdr(msg):  print(f"\n{BOLD}{CYAN}{msg}{RESET}")
def row(msg):  print(f"  {msg}")

# ── connect ────────────────────────────────────────────────────────────────
print(f"\n{BOLD}Connecting to database…{RESET}")
engine = get_engine()

with engine.connect() as conn:

    # list all tables
    tables = [r[0] for r in conn.execute(text("SHOW TABLES")).fetchall()]
    print(f"{GREEN}✔ Connected — {len(tables)} table(s) found{RESET}")

    for table in sorted(tables):

        # row count
        count = conn.execute(text(f"SELECT COUNT(*) FROM `{table}`")).scalar()

        hdr(f"┌─ {table}  {DIM}({count:,} rows){RESET}")

        # columns
        columns = conn.execute(text(f"DESCRIBE `{table}`")).fetchall()
        # col: Field | Type | Null | Key | Default | Extra
        col_w = max(len(c[0]) for c in columns) + 2

        row(f"{'COLUMN':<{col_w}}  {'TYPE':<28}  {'NULL':<5}  {'KEY':<4}  DEFAULT")
        row(f"{'─'*col_w}  {'─'*28}  {'─'*5}  {'─'*4}  {'─'*12}")

        for c in columns:
            field, typ, null, key, default, extra = c
            key_tag   = f"{YELLOW}{key}{RESET}"  if key   else "    "
            extra_tag = f" {DIM}{extra}{RESET}"   if extra else ""
            default_s = str(default) if default is not None else DIM + "NULL" + RESET
            print(
                f"  {BOLD}{field:<{col_w}}{RESET}"
                f"  {typ:<28}"
                f"  {null:<5}"
                f"  {key_tag:<4}"
                f"  {default_s}{extra_tag}"
            )

        # indexes
        indexes = conn.execute(text(f"SHOW INDEX FROM `{table}`")).fetchall()
        if indexes:
            seen: dict[str, list[str]] = {}
            for idx in indexes:
                name, col = idx[2], idx[4]
                seen.setdefault(name, []).append(col)
            row("")
            row(f"{DIM}Indexes:{RESET}")
            for name, cols in seen.items():
                row(f"  {DIM}{name:<30}  ({', '.join(cols)}){RESET}")

print(f"\n{BOLD}{GREEN}Done.{RESET}\n")
