#!/usr/bin/env python3
"""
Smoke-test engagement against the real Sendy DB (uses .env like the app).

  python test_engagement_flow.py

Picks subscribers who actually have queue deliveries (not just newest IDs —
those often have sent=0 / checked=0). Prints timings and sample rows.

Optional env (same idea as the dashboard):
  DATE_FROM=YYYY-MM-DD  DATE_TO=YYYY-MM-DD
"""

from __future__ import annotations

import os
import sys
import time

from dotenv import load_dotenv

load_dotenv()

from db import fetch_data  # noqa: E402
import queries  # noqa: E402


def _env_date(name: str) -> str | None:
    v = (os.getenv(name) or "").strip()
    return v or None


def fetch_ids_with_queue_deliveries(limit: int = 20) -> list[int]:
    """Subscriber IDs with at least one queue row tied to a *sent* campaign."""
    sql = """
        SELECT DISTINCT q.subscriber_id AS id
        FROM   queue q
        INNER JOIN campaigns c ON c.id = q.campaign_id
        WHERE  q.sent >= 1
          AND  c.sent IS NOT NULL
          AND  c.sent NOT IN ('', '0')
        ORDER  BY q.subscriber_id DESC
        LIMIT  :lim
    """
    df = fetch_data(sql, {"lim": limit})
    return [int(x) for x in df["id"].tolist()]


def fetch_recent_subscriber_ids(limit: int = 20) -> list[int]:
    df = fetch_data("SELECT id FROM subscribers ORDER BY id DESC LIMIT :lim", {"lim": limit})
    return [int(x) for x in df["id"].tolist()]


def print_block(title: str, ids: list[int], date_from: str | None, date_to: str | None) -> None:
    if not ids:
        print(f"\n{title}: (no ids)\n")
        return
    t0 = time.perf_counter()
    data, meta = queries.build_engagement_for_subscribers(ids, date_from=date_from, date_to=date_to)
    elapsed = time.perf_counter() - t0
    print(f"\n=== {title} ({len(ids)} ids) ===")
    print("engagement_meta:", meta)
    print(f"build_engagement_for_subscribers in {elapsed:.2f}s\n")
    for sid in ids[:10]:
        row = data.get(str(sid), {})
        print(
            f"  id={sid}  sent={row.get('emails_sent')}  opens={row.get('opens')}  "
            f"clicks={row.get('clicks')}  open%={row.get('open_pct')}  "
            f"click%={row.get('click_pct')}  checked={row.get('checked')}"
        )


def main() -> int:
    date_from = _env_date("DATE_FROM")
    date_to = _env_date("DATE_TO")
    if date_from or date_to:
        print(f"Date filter: DATE_FROM={date_from!r} DATE_TO={date_to!r}\n")

    try:
        queue_ids = fetch_ids_with_queue_deliveries(20)
        recent_ids = fetch_recent_subscriber_ids(20)
    except Exception as exc:
        print("DB error:", exc)
        return 1

    if not queue_ids and not recent_ids:
        print("No subscribers found.")
        return 1

    # These IDs should usually show non-zero sent and often non-zero checked/opens.
    print_block(
        "Subscribers with queue deliveries (realistic sample)",
        queue_ids,
        date_from,
        date_to,
    )

    print_block(
        "Newest subscribers by id (often all zeros; not a bug)",
        recent_ids,
        date_from,
        date_to,
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
