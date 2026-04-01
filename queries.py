"""
Centralised SQL query definitions — corrected for actual Sendy schema.

opens / clicks column format (discovered via show_schema + debug)
----------------------------------------------------------------
campaigns.lists  = comma-separated list IDs targeted by the send (per Sendy DB)
campaigns.opens  = 'sub_id:country,sub_id:country,...'  (comma-separated)
links.clicks     = 'sub_id,sub_id,...'                  (comma-separated)

Count formula: LENGTH(col) - LENGTH(REPLACE(col, ',', '')) + 1
(one comma = two entries, so +1 gives the correct element count)

When ``USE_DENORM_COUNTS`` is True, aggregates use ``campaigns.opens_count`` and
``links.clicks_count`` (see ``migrations/004_denorm_opens_clicks_counts.sql``).
"""

import logging
import os
import re
from datetime import date, datetime, timedelta

import pandas as pd
from flask import g
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from cache import cached
from db import fetch_data

logger = logging.getLogger(__name__)

# Max rows returned by ``get_campaign_performance`` (dashboard campaign name list / stats)
CAMPAIGN_PERF_MAX = max(200, min(10_000, int(os.getenv("CAMPAIGN_PERF_MAX", "5000"))))
CAMPAIGN_PERF_DEFAULT = max(50, min(CAMPAIGN_PERF_MAX, int(os.getenv("CAMPAIGN_PERF_DEFAULT", "2500"))))
# Avg Open/Click **summary cards** only — parses ``opens``/``links`` per row; keep low on remote MySQL.
CAMPAIGN_ENGAGEMENT_SUMMARY_MAX = max(
    30, min(1000, int(os.getenv("CAMPAIGN_ENGAGEMENT_SUMMARY_MAX", "180")))
)

# OPTIMIZED: capped env var chunk sizes to prevent runaway query loads
CAMPAIGN_PERF_DETAIL_CHUNK = min(int(os.getenv("CAMPAIGN_PERF_DETAIL_CHUNK", "50")), 100)
USERS_EMAIL_FETCH_BATCH = min(int(os.getenv("USERS_EMAIL_FETCH_BATCH", "500")), 2000)
USERS_EMAIL_IN_CHUNK = min(int(os.getenv("USERS_EMAIL_IN_CHUNK", "200")), 500)
USERS_EMAIL_MAX_ROWS_SCAN = min(int(os.getenv("USERS_EMAIL_MAX_ROWS_SCAN", "50000")), 200000)
ENGAGEMENT_PARSE_CAMPAIGNS = min(int(os.getenv("ENGAGEMENT_PARSE_CAMPAIGNS", "200")), 500)
# ``get_engagement_map``: fetch ``lists`` in batches (remote MySQL — avoid one multi‑MB result set).
ENGAGEMENT_LISTS_FETCH_BATCH = min(
    max(int(os.getenv("ENGAGEMENT_LISTS_FETCH_BATCH", "300")), 50),
    2000,
)

# Subscriber growth chart: split wide ``date_from``/``date_to`` into smaller queries
# so remote MySQL does not hit max_execution_time / drop the socket (2013).
# 0 = single query (old behaviour).
SUBSCRIBER_STATS_CHUNK_DAYS = max(
    0, min(366, int(os.getenv("SUBSCRIBER_STATS_CHUNK_DAYS", "45")))
)

# Use ``opens_count`` / ``clicks_count`` instead of LENGTH/REPLACE on longtext at query time.
USE_DENORM_COUNTS = True  # set False to revert

_SENT_FILTER = "sent IS NOT NULL AND sent NOT IN ('', '0')"

# Prefer internal title, then subject/label; last resort = "Campaign #id"
_CAMPAIGN_DISPLAY_SQL = """
COALESCE(
    NULLIF(TRIM(c.title), ''),
    NULLIF(TRIM(c.label), ''),
    CONCAT('Campaign #', CAST(c.id AS CHAR))
)
""".strip()

_PERIOD_FORMATS: dict[str, str] = {
    "day":   "%Y-%m-%d",
    "month": "%Y-%m",
}


def _parse_campaign_list_ids(raw: object) -> frozenset[int]:
    """
    Parse ``campaigns.lists`` (mediumtext): comma-separated IDs and/or PHP
    serialized arrays, e.g. ``a:1:{i:0;i:308;}`` (real Sendy schema).
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return frozenset()
    s = str(raw).strip()
    if not s:
        return frozenset()
    out: set[int] = set()

    # Plain comma-separated (no PHP structure)
    if ";" not in s and "{" not in s and "(" not in s:
        for part in s.split(","):
            p = part.strip()
            if p.isdigit():
                out.add(int(p))
        return frozenset(out)

    # PHP serialized int array: i:0;i:308;i:1;i:2130;
    for m in re.finditer(r"i:\d+;i:(\d+);", s):
        out.add(int(m.group(1)))
    # String elements: s:3:"308"
    for m in re.finditer(r's:\d+:"(\d+)"', s):
        out.add(int(m.group(1)))
    # Mixed / fallback: digits after commas
    if not out:
        for part in s.split(","):
            p = re.sub(r"[^\d]", "", part)
            if p.isdigit():
                out.add(int(p))
    return frozenset(out)


def campaign_effective_unix_ts_sql(table_alias: str) -> str:
    """
    SQL expression: Unix timestamp for when a campaign was sent.

    Backed by the ``campaigns.effective_ts`` STORED GENERATED column
    (see ``migrations/001_campaigns_effective_ts.sql``) so ``WHERE``,
    ``ORDER BY``, and range scans can use ``idx_camp_effective_ts``.
    """
    p = f"{table_alias}." if table_alias else ""
    return f"{p}effective_ts"


def build_campaign_where_sent_in_range(
    table_alias: str,
    date_from: str | None,
    date_to: str | None,
    params: dict | None = None,
) -> tuple[str, dict]:
    """
    AND-joined conditions: row is a *sent* campaign whose send time falls in
    the inclusive calendar range.

    Sendy ``campaigns.sent`` is varchar: usually a Unix epoch string, but often
    ``'1'`` once sent — then use ``send_date`` (epoch string or ``YYYY-MM-DD``).
    """
    if params is None:
        params = {}
    else:
        params = dict(params)
    p = f"{table_alias}." if table_alias else ""
    ts = campaign_effective_unix_ts_sql(table_alias)
    parts = [f"{p}sent IS NOT NULL", f"{p}sent NOT IN ('', '0')"]
    # Exclude rows we cannot place in time when a date filter is requested
    if date_from or date_to:
        parts.append(f"({ts}) IS NOT NULL")
    if date_from:
        params["date_from"] = date_from
        parts.append(f"({ts}) >= UNIX_TIMESTAMP(:date_from)")
    if date_to:
        params["date_to"] = date_to
        parts.append(f"({ts}) < UNIX_TIMESTAMP(DATE_ADD(:date_to, INTERVAL 1 DAY))")
    return " AND ".join(parts), params


def get_subscriber_list_ids(sub_ids: list[int]) -> dict[int, int]:
    """Map subscriber id → ``subscribers.list`` (mailing list id)."""
    if not sub_ids:
        return {}
    placeholders = ", ".join(f":_s{i}" for i in range(len(sub_ids)))
    params = {f"_s{i}": v for i, v in enumerate(sub_ids)}
    sql = f"SELECT id, `list` AS list_id FROM subscribers WHERE id IN ({placeholders})"
    df = fetch_data(sql, params)
    return {int(r["id"]): int(r["list_id"]) for _, r in df.iterrows()}


def _comma_count_expr(col: str) -> str:
    """
    SQL expression: count comma-separated entries in *col* (longtext scan).
    Returns 0 for NULL / empty string.
    """
    return (
        f"CASE WHEN {col} IS NULL OR {col} = '' THEN 0 "
        f"ELSE LENGTH({col}) - LENGTH(REPLACE({col}, ',', '')) + 1 END"
    )


def _comma_count_col(table_alias: str, col_type: str) -> str:
    """
    Comma-delimited entry count for ``opens`` or ``clicks`` on *table_alias*.

    *col_type* — ``'opens'`` (``campaigns``) or ``'clicks'`` (``links``).
    When ``USE_DENORM_COUNTS`` is True, returns the stored counter column name.
    """
    p = f"{table_alias}."
    if USE_DENORM_COUNTS:
        if col_type == "opens":
            return f"{p}opens_count"
        if col_type == "clicks":
            return f"{p}clicks_count"
        raise ValueError(f"unknown col_type: {col_type!r}")
    if col_type == "opens":
        return _comma_count_expr(f"{p}opens")
    if col_type == "clicks":
        return _comma_count_expr(f"{p}clicks")
    raise ValueError(f"unknown col_type: {col_type!r}")


def _comma_count(col: str) -> str:
    """
    Count comma-separated entries for ``c.opens`` / ``lk.clicks``, or any *col*
    via ``_comma_count_expr`` when denormalized columns do not apply.
    """
    if USE_DENORM_COUNTS:
        if col == "c.opens":
            return "c.opens_count"
        if col == "lk.clicks":
            return "lk.clicks_count"
    return _comma_count_expr(col)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_where(conditions: list[tuple[str, str, object]]) -> tuple[str, dict]:
    """Build a parameterised WHERE clause, skipping None values."""
    clauses: list[str] = []
    params: dict = {}
    for sql_frag, param_name, value in conditions:
        if value is not None:
            clauses.append(sql_frag)
            params[param_name] = value
    return ("WHERE " + " AND ".join(clauses)) if clauses else "", params


# ---------------------------------------------------------------------------
# Generic
# ---------------------------------------------------------------------------

def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Execute an arbitrary SQL string and return a DataFrame."""
    return fetch_data(sql, params)


# ---------------------------------------------------------------------------
# Subscribers
# ---------------------------------------------------------------------------

def get_all_subscribers(limit: int = 100, offset: int = 0) -> pd.DataFrame:
    """Return subscriber rows, paginated."""
    sql = """
        SELECT
            id,
            email,
            name,
            `list`                                  AS list_id,
            confirmed,
            bounced,
            unsubscribed,
            FROM_UNIXTIME(timestamp)                AS created_at
        FROM   subscribers
        ORDER  BY timestamp DESC
        LIMIT  :_limit OFFSET :_offset
    """
    return fetch_data(sql, {"_limit": limit, "_offset": offset})


def get_subscribers_by_list(list_id: int, limit: int = 100, offset: int = 0) -> pd.DataFrame:
    """Return subscribers belonging to *list_id*, paginated."""
    if not isinstance(list_id, int) or list_id <= 0:
        raise ValueError(f"list_id must be a positive integer, got {list_id!r}")
    sql = """
        SELECT
            id, email, name,
            `list`                                  AS list_id,
            confirmed, bounced, unsubscribed,
            FROM_UNIXTIME(timestamp)                AS created_at
        FROM   subscribers
        WHERE  `list` = :list_id
        ORDER  BY timestamp DESC
        LIMIT  :_limit OFFSET :_offset
    """
    return fetch_data(sql, {"list_id": list_id, "_limit": limit, "_offset": offset})


@cached(ttl=120)
def get_active_subscriber_counts() -> pd.DataFrame:
    """Return per-list active subscriber counts (cached 2 min)."""
    sql = """
        SELECT `list` AS list_id, COUNT(*) AS active_count
        FROM   subscribers
        WHERE  unsubscribed = 0 AND bounced = 0 AND complaint = 0
        GROUP  BY `list`
    """
    return fetch_data(sql)


def _email_search_pattern(raw: str | None) -> str | None:
    """Build a safe LIKE pattern; strips SQL-wildcard chars from user input."""
    if raw is None:
        return None
    t = str(raw).strip()
    if not t:
        return None
    safe = re.sub(r"[%_\\]", "", t)
    if not safe:
        return None
    return f"%{safe.lower()}%"


def _subscriber_where_fragment(
    table_alias: str,
    date_from: str | None,
    date_to: str | None,
    list_id: int | None,
    status: str | None,
    email_search: str | None,
    *,
    require_nonblank_email: bool = False,
) -> tuple[str, dict]:
    """
    AND-joined SQL fragment for ``subscribers`` filters (with optional table alias).

    *require_nonblank_email* — exclude NULL/blank emails (for ``GROUP BY email``).
    """
    p = f"{table_alias}." if table_alias else ""
    conditions, params = [], {}
    if date_from:
        conditions.append(f"{p}timestamp >= UNIX_TIMESTAMP(:date_from)")
        params["date_from"] = date_from
    if date_to:
        conditions.append(
            f"{p}timestamp < UNIX_TIMESTAMP(DATE_ADD(:date_to, INTERVAL 1 DAY))"
        )
        params["date_to"] = date_to
    if list_id is not None:
        conditions.append(f"{p}`list` = :list_id")
        params["list_id"] = list_id
    if status is not None:
        conditions.append(f"{p}confirmed = :confirmed")
        params["confirmed"] = 1 if str(status) in ("1", "confirmed", "active") else 0
    pat = _email_search_pattern(email_search)
    if pat is not None:
        conditions.append(f"{p}email_lower LIKE :email_like")
        params["email_like"] = pat
    if require_nonblank_email:
        conditions.append(f"TRIM(COALESCE({p}email, '')) != ''")
    if not conditions:
        return "1=1", params
    return " AND ".join(conditions), params


@cached(ttl=120)
def count_users(
    date_from: str | None = None,
    date_to: str | None = None,
    list_id: int | None = None,
    status: str | None = None,
    email_search: str | None = None,
) -> int:
    """Total subscriber count matching filters (cached 2 min).

    subscribers.timestamp is a Unix int — date comparisons use UNIX_TIMESTAMP().
    """
    frag, params = _subscriber_where_fragment(
        "", date_from, date_to, list_id, status, email_search,
    )
    df = fetch_data(f"SELECT COUNT(*) AS total FROM subscribers WHERE {frag}", params)
    return int(df.iloc[0]["total"]) if len(df) else 0


@cached(ttl=600)  # OPTIMIZED: increased TTL to reduce HOT query frequency
def count_users_distinct_email(
    date_from: str | None = None,
    date_to: str | None = None,
    list_id: int | None = None,
    status: str | None = None,
    email_search: str | None = None,
) -> int:
    """Distinct non-blank emails (via ``email_lower``) — same filters as ``count_users``."""
    frag, params = _subscriber_where_fragment(
        "", date_from, date_to, list_id, status, email_search,
        require_nonblank_email=True,
    )
    sql = f"""
        SELECT COUNT(DISTINCT email_lower) AS n
        FROM   subscribers
        WHERE  {frag}
    """
    df = fetch_data(sql, params)
    return int(df.iloc[0]["n"]) if len(df) else 0


@cached(ttl=600)
def get_mailing_lists() -> pd.DataFrame:
    """All lists ``id`` + ``name`` for filters and display (Sendy ``lists`` table)."""
    sql = """
        SELECT id, name
        FROM   lists
        ORDER  BY name ASC
    """
    return fetch_data(sql, {})


@cached(ttl=30)
def get_users(
    date_from: str | None = None,
    date_to: str | None = None,
    list_id: int | None = None,
    status: str | None = None,
    email_search: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> pd.DataFrame:
    """Return a page of subscriber rows with optional filters (cached 30 s)."""
    frag, params = _subscriber_where_fragment(
        "s", date_from, date_to, list_id, status, email_search,
    )
    params["_limit"]  = limit
    params["_offset"] = offset

    sql = f"""
        SELECT
            s.id,
            s.email,
            s.name,
            s.`list`                                AS list_id,
            li.name                                 AS list_name,
            s.confirmed,
            s.unsubscribed,
            s.bounced,
            DATE(FROM_UNIXTIME(s.timestamp))       AS joined_date
        FROM   subscribers s
        LEFT   JOIN lists li ON li.id = s.`list`
        WHERE  {frag}
        ORDER  BY s.timestamp DESC
        LIMIT  :_limit OFFSET :_offset
    """
    return fetch_data(sql, params)


def _norm_email_key(raw: object) -> str:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    return str(raw).strip().lower()


def get_users_by_email(
    date_from: str | None = None,
    date_to: str | None = None,
    list_id: int | None = None,
    status: str | None = None,
    email_search: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[pd.DataFrame, bool]:
    """
    One row per distinct email (``LOWER(TRIM(email))``), same filters as ``get_users``.

    Scans ``subscribers`` in ``timestamp DESC`` batches — avoids ``GROUP BY`` over
    the whole filtered set (which drops remote MySQL on large tables).

    Returns
    -------
    DataFrame
        ``id`` = smallest subscriber id in the group; ``subscriber_ids`` = all ids.
    has_more
        ``True`` if another page of distinct emails likely exists.
    """
    empty_cols = [
        "id",
        "email",
        "name",
        "list_id",
        "list_name",
        "confirmed",
        "unsubscribed",
        "bounced",
        "joined_date",
        "subscriber_ids",
    ]
    empty = pd.DataFrame(columns=empty_cols)

    frag, params = _subscriber_where_fragment(
        "s", date_from, date_to, list_id, status, email_search,
        require_nonblank_email=True,
    )
    need = offset + limit + 1
    seen_order: list[str] = []
    seen_set: set[str] = set()
    # First row’s stored ``s.email`` per normalized key — second phase uses
    # ``s.email IN (...)`` so MySQL can use ``s_email``; fallback uses
    # ``s.email_lower IN (...)`` (indexed) when canonical addresses are missing.
    key_to_canonical: dict[str, str] = {}

    batch = USERS_EMAIL_FETCH_BATCH
    max_scan = USERS_EMAIL_MAX_ROWS_SCAN
    sql_offset = 0
    total_scanned = 0
    hit_row_cap = False

    while len(seen_order) < need and total_scanned < max_scan:
        p = dict(params)
        p["_lim"] = batch
        p["_off"] = sql_offset
        sql = f"""
            SELECT
                s.id,
                s.email,
                s.name,
                s.`list`                                AS list_id,
                li.name                                 AS list_name,
                s.confirmed,
                s.unsubscribed,
                s.bounced,
                DATE(FROM_UNIXTIME(s.timestamp))       AS joined_date,
                LOWER(TRIM(s.email))                    AS email_key
            FROM   subscribers s
            LEFT   JOIN lists li ON li.id = s.`list`
            WHERE  {frag}
            ORDER  BY s.timestamp DESC, s.id DESC
            LIMIT  :_lim OFFSET :_off
        """
        df = fetch_data(sql, p)
        n = len(df) if df is not None else 0
        if n == 0:
            break
        for _, row in df.iterrows():
            ek = _norm_email_key(row.get("email_key"))
            if not ek:
                continue
            if ek not in seen_set:
                seen_set.add(ek)
                raw_em = str(row.get("email") or "").strip()
                if raw_em:
                    key_to_canonical[ek] = raw_em
                seen_order.append(ek)
                if len(seen_order) >= need:
                    break
        total_scanned += n
        sql_offset += n
        if n < batch:
            break
        if total_scanned >= max_scan and n == batch:
            hit_row_cap = True
            logger.warning(
                "get_users_by_email: stopped at scan cap (%s subscriber rows); "
                "raise USERS_EMAIL_MAX_ROWS_SCAN if pages look truncated.",
                max_scan,
            )
            break

    page_keys = seen_order[offset : offset + limit]
    if not page_keys:
        has_more = (len(seen_order) > offset + limit) or hit_row_cap
        return empty, has_more

    frag2, p_in = _subscriber_where_fragment(
        "s", date_from, date_to, list_id, status, email_search,
        require_nonblank_email=True,
    )

    chunk_sz = USERS_EMAIL_IN_CHUNK
    parts: list[pd.DataFrame] = []

    def _fetch_user_chunk_by_emails(emails: list[str]) -> pd.DataFrame | None:
        if not emails:
            return None
        ip = dict(p_in)
        ph = ", ".join(f":_em{i}" for i in range(len(emails)))
        for i, em in enumerate(emails):
            ip[f"_em{i}"] = em
        wf = f"{frag2} AND s.email IN ({ph})"
        q = f"""
            SELECT
                s.id,
                s.email,
                s.name,
                s.`list`                                AS list_id,
                li.name                                 AS list_name,
                s.confirmed,
                s.unsubscribed,
                s.bounced,
                DATE(FROM_UNIXTIME(s.timestamp))       AS joined_date,
                LOWER(TRIM(s.email))                    AS email_key
            FROM   subscribers s
            LEFT   JOIN lists li ON li.id = s.`list`
            WHERE  {wf}
        """
        return fetch_data(q, ip)

    def _fetch_user_chunk_by_keys(keys: list[str]) -> pd.DataFrame | None:
        """Fallback when no canonical address was stored (rare)."""
        if not keys:
            return None
        ip = dict(p_in)
        ph = ", ".join(f":_ek{i}" for i in range(len(keys)))
        for i, k in enumerate(keys):
            ip[f"_ek{i}"] = k
        wf = f"{frag2} AND s.email_lower IN ({ph})"
        q = f"""
            SELECT
                s.id,
                s.email,
                s.name,
                s.`list`                                AS list_id,
                li.name                                 AS list_name,
                s.confirmed,
                s.unsubscribed,
                s.bounced,
                DATE(FROM_UNIXTIME(s.timestamp))       AS joined_date,
                LOWER(TRIM(s.email))                    AS email_key
            FROM   subscribers s
            LEFT   JOIN lists li ON li.id = s.`list`
            WHERE  {wf}
        """
        return fetch_data(q, ip)

    for start in range(0, len(page_keys), chunk_sz):
        chunk_keys = page_keys[start : start + chunk_sz]
        canon = [key_to_canonical.get(k, "").strip() for k in chunk_keys]
        with_canon = [e for e in canon if e]
        missing_keys = [k for k, e in zip(chunk_keys, canon) if not e]

        if with_canon:
            df_part = _fetch_user_chunk_by_emails(with_canon)
            if df_part is not None and len(df_part) > 0:
                parts.append(df_part)
        if missing_keys:
            df_fb = _fetch_user_chunk_by_keys(missing_keys)
            if df_fb is not None and len(df_fb) > 0:
                parts.append(df_fb)

    if not parts:
        return empty, False
    df_full = pd.concat(parts, ignore_index=True)

    df_full = df_full.copy()
    df_full["email_key"] = df_full["email_key"].astype(str)
    keys = [str(k) for k in page_keys]

    grouped = df_full.groupby("email_key", sort=False)
    rows: list[dict] = []
    for ek in keys:
        if ek not in grouped.groups:
            continue
        g = grouped.get_group(ek)
        ids = [int(x) for x in g["id"].tolist()]
        rep = min(ids)
        first = g.iloc[0]
        rows.append(
            {
                "id": rep,
                "email": first["email"],
                "name": first.get("name"),
                "list_id": first.get("list_id"),
                "list_name": None,
                "confirmed": int(first["confirmed"]) if pd.notna(first.get("confirmed")) else 0,
                "unsubscribed": int(first["unsubscribed"]) if pd.notna(first.get("unsubscribed")) else 0,
                "bounced": int(first["bounced"]) if pd.notna(first.get("bounced")) else 0,
                "joined_date": first.get("joined_date"),
                "subscriber_ids": ids,
            }
        )

    has_more = len(seen_order) > offset + limit or hit_row_cap
    return pd.DataFrame(rows), has_more


# ---------------------------------------------------------------------------
# Stats – subscriber growth
# ---------------------------------------------------------------------------

def _fetch_subscriber_stats_once(
    date_from: str | None,
    date_to: str | None,
    list_id: int | None,
    group_by: str,
    fmt: str,
) -> pd.DataFrame:
    """One DB round-trip for subscriber growth (used alone or per chunk)."""
    conditions, params = [], {}
    if date_from:
        conditions.append("timestamp >= UNIX_TIMESTAMP(:date_from)")
        params["date_from"] = date_from
    if date_to:
        conditions.append("timestamp < UNIX_TIMESTAMP(DATE_ADD(:date_to, INTERVAL 1 DAY))")
        params["date_to"] = date_to
    if list_id is not None:
        conditions.append("`list` = :list_id")
        params["list_id"] = list_id

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    if list_id is not None:
        group_sql = "GROUP BY period, `list`"
        select_extra = ",\n            `list`  AS list_id"
    else:
        group_sql = "GROUP BY period"
        select_extra = ""

    sql = f"""
        SELECT
            DATE_FORMAT(FROM_UNIXTIME(`timestamp`), '{fmt}') AS period
            {select_extra},
            COUNT(*)                                        AS new_subscribers,
            SUM(bounced)                                    AS bounced,
            SUM(unsubscribed)                               AS unsubscribed,
            COUNT(*) - SUM(bounced) - SUM(unsubscribed)     AS net_active
        FROM   subscribers
        {where}
        {group_sql}
        ORDER  BY period DESC
    """
    return fetch_data(sql, params)


def _merge_subscriber_stat_chunks(dfs: list[pd.DataFrame], has_list: bool) -> pd.DataFrame:
    """Sum metrics for duplicate period (and list) keys after chunked queries."""
    parts = [d for d in dfs if d is not None and len(d) > 0]
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    agg = {
        "new_subscribers": "sum",
        "bounced": "sum",
        "unsubscribed": "sum",
    }
    if has_list:
        out = df.groupby(["period", "list_id"], as_index=False).agg(agg)
    else:
        out = df.groupby("period", as_index=False).agg(agg)
    out["net_active"] = (
        out["new_subscribers"] - out["bounced"] - out["unsubscribed"]
    )
    return out.sort_values("period", ascending=False).reset_index(drop=True)


def _subscriber_stats_date_chunks(d0: date, d1: date, chunk_days: int) -> list[tuple[str, str]]:
    """Inclusive calendar ranges as YYYY-MM-DD strings."""
    out: list[tuple[str, str]] = []
    cur = d0
    while cur <= d1:
        c_end = min(cur + timedelta(days=chunk_days - 1), d1)
        out.append((cur.isoformat(), c_end.isoformat()))
        cur = c_end + timedelta(days=1)
    return out


@cached(ttl=300)
def get_subscriber_stats(
    date_from: str | None = None,
    date_to: str | None = None,
    list_id: int | None = None,
    group_by: str = "day",
) -> pd.DataFrame:
    """
    Subscriber growth aggregated by time period.

    When *list_id* is None, results are NOT grouped by list — this avoids a
    2 001-list × 90-day cross-product that can produce 180 000+ rows and take
    15+ seconds.  Pass *list_id* to get per-list breakdown for a single list.

    subscribers.timestamp is a Unix int — filters use UNIX_TIMESTAMP().

    Wide ``date_from``/``date_to`` ranges are split into chunks of
    ``SUBSCRIBER_STATS_CHUNK_DAYS`` (default 45, ``0`` = one query) so large
    ``GROUP BY`` scans do not trigger MySQL socket drops (2013).
    """
    fmt = _PERIOD_FORMATS.get(group_by)
    if fmt is None:
        raise ValueError(
            f"'group_by' must be one of {list(_PERIOD_FORMATS)}, got {group_by!r}"
        )

    chunk = SUBSCRIBER_STATS_CHUNK_DAYS
    use_chunks = (
        chunk > 0
        and date_from
        and date_to
        and str(date_from).strip()
        and str(date_to).strip()
    )
    if use_chunks:
        try:
            d0 = datetime.strptime(str(date_from).strip()[:10], "%Y-%m-%d").date()
            d1 = datetime.strptime(str(date_to).strip()[:10], "%Y-%m-%d").date()
        except ValueError:
            use_chunks = False
        else:
            span = (d1 - d0).days + 1
            if span <= chunk:
                use_chunks = False

    if not use_chunks:
        return _fetch_subscriber_stats_once(date_from, date_to, list_id, group_by, fmt)

    ranges = _subscriber_stats_date_chunks(d0, d1, chunk)
    logger.info(
        "subscriber stats: %d chunk(s) of up to %d day(s) for %s → %s",
        len(ranges),
        chunk,
        date_from,
        date_to,
    )
    dfs: list[pd.DataFrame] = []
    for cf, ct in ranges:
        dfs.append(_fetch_subscriber_stats_once(cf, ct, list_id, group_by, fmt))
    return _merge_subscriber_stat_chunks(dfs, list_id is not None)


# ---------------------------------------------------------------------------
# Campaigns
# ---------------------------------------------------------------------------

def get_all_campaigns(limit: int = 100, offset: int = 0) -> pd.DataFrame:
    """Return campaign rows, paginated."""
    sql = """
        SELECT
            id, title, label AS subject,
            from_name, from_email, reply_to,
            sent, opens_tracking, links_tracking, send_date
        FROM   campaigns
        ORDER  BY id DESC
        LIMIT  :_limit OFFSET :_offset
    """
    return fetch_data(sql, {"_limit": limit, "_offset": offset})


def get_campaign_by_id(campaign_id: int) -> pd.DataFrame:
    """Return a single campaign row by *campaign_id*."""
    if not isinstance(campaign_id, int) or campaign_id <= 0:
        raise ValueError(f"campaign_id must be a positive integer, got {campaign_id!r}")
    sql = """
        SELECT id, title, label AS subject,
               from_name, from_email, reply_to, sent, send_date
        FROM   campaigns
        WHERE  id = :campaign_id
    """
    return fetch_data(sql, {"campaign_id": campaign_id})


@cached(ttl=300)  # OPTIMIZED: increased TTL to reduce HOT query frequency
def get_campaign_stats() -> pd.DataFrame:
    """Aggregated open / click stats per sent campaign (cached 5 min).

    Uses a pre-aggregated JOIN on links (10 K rows) — no correlated subqueries.
    Uses ``_comma_count`` on native Sendy ``opens`` / ``clicks`` longtext (no shadow columns).
    """
    opens_expr = _comma_count("c.opens")
    clicks_expr = _comma_count("lk.clicks")
    sql = f"""
        SELECT
            c.id                                                AS campaign_id,
            c.title,
            c.label                                             AS subject,
            c.recipients                                        AS sent,
            COALESCE({opens_expr},  0)                          AS opens,
            COALESCE(lnk.total_clicks, 0)                       AS clicks,
            ROUND({opens_expr} * 100.0 / NULLIF(c.recipients, 0), 2) AS open_rate_pct
        FROM campaigns c
        LEFT JOIN (
            SELECT lk.campaign_id,
                   SUM({clicks_expr}) AS total_clicks
            FROM   links lk
            GROUP  BY lk.campaign_id
        ) lnk ON lnk.campaign_id = c.id
        WHERE  {_SENT_FILTER}
        ORDER  BY c.id DESC
        LIMIT  200
    """
    return fetch_data(sql, {})


# ---------------------------------------------------------------------------
# Stats – campaign performance  (generic dashboard /api/stats?type=campaigns)
# ---------------------------------------------------------------------------

def get_email_totals(
    date_from: str | None = None,
    date_to: str | None = None,
) -> pd.DataFrame:
    """
    Aggregate total emails sent and campaign count for the selected date range.

    Intentionally **not** cached so dashboard totals track filter changes immediately.

    Uses ``sent`` / ``send_date`` via ``build_campaign_where_sent_in_range`` so
    ``sent = '1'`` installs still filter correctly (see Sendy schema).
    """
    clause, params = build_campaign_where_sent_in_range("", date_from, date_to, {})
    where = "WHERE " + clause
    sql = f"""
        SELECT
            COUNT(*)                      AS campaigns_sent,
            COALESCE(SUM(recipients), 0)  AS total_emails_sent
        FROM campaigns
        {where}
    """
    return fetch_data(sql, params)


@cached(ttl=600)  # OPTIMIZED: increased TTL to reduce HOT query frequency
def get_engagement_map(
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict:
    """
    Build data for per-subscriber engagement (Sendy schema).

    - **Emails sent (per subscriber)** — count of sent campaigns in the date
      range whose ``campaigns.lists`` includes that subscriber's ``list``
      (comma-separated list IDs in Sendy).  If ``lists`` is missing, falls
      back to *total* campaigns in range for everyone (legacy behaviour).

    - **Opens / clicks** — parsed from ``campaigns.opens`` and ``links.clicks``
      (comma-separated ``sub_id:country`` / ``sub_id``) for the **last 100**
      sent campaigns in the range, scoped to campaigns that target the
      subscriber's list when ``lists`` is available.

    Parsing is done in Python to avoid REGEXP on longtext at SQL scale.
    """
    clause, params = build_campaign_where_sent_in_range("", date_from, date_to, {})
    where = "WHERE " + clause

    df_n = fetch_data(f"SELECT COUNT(*) AS n FROM campaigns {where}", params)
    total_campaigns = int(df_n.iloc[0]["n"]) if len(df_n) else 0

    uses_lists_column = True
    emails_sent_by_list: dict[int, int] = {}
    try:
        # OPTIMIZED: batched ``lists`` fetch for large date ranges (remote DB + narrow result sets).
        _lb = ENGAGEMENT_LISTS_FETCH_BATCH
        _cursor: int | None = None
        while True:
            if _cursor is None:
                _extra, _p = "", dict(params)
            else:
                _extra = " AND id < :_eng_lists_cursor "
                _p = dict(params)
                _p["_eng_lists_cursor"] = _cursor
            _sql_lc = (
                f"SELECT id, lists FROM campaigns {where}{_extra} "
                f"ORDER BY id DESC LIMIT {_lb}"
            )
            df_lc = fetch_data(_sql_lc, _p)
            if df_lc is None or len(df_lc) == 0:
                break
            for _, row in df_lc.iterrows():
                for lid in _parse_campaign_list_ids(row.get("lists")):
                    emails_sent_by_list[lid] = emails_sent_by_list.get(lid, 0) + 1
            _cursor = int(df_lc["id"].min())
            if len(df_lc) < _lb:
                break
    except OperationalError as exc:
        orig = str(getattr(exc, "orig", exc))
        if "lists" in orig.lower() and "unknown column" in orig.lower():
            uses_lists_column = False
            logger.warning(
                "campaigns.lists not found — emails_sent uses total campaigns in range"
            )
        else:
            raise

    if total_campaigns == 0:
        return {
            "emails_sent_by_list": emails_sent_by_list,
            "campaigns":           [],
            "total_campaigns":     0,
            "uses_lists_column":   uses_lists_column,
            "scope_by_list":       False,
        }

    camp_cols = "id, lists, opens" if uses_lists_column else "id, opens"
    lim = ENGAGEMENT_PARSE_CAMPAIGNS
    try:
        df_camps = fetch_data(
            f"SELECT {camp_cols} FROM campaigns {where} ORDER BY id DESC LIMIT {lim}",
            params,
        )
    except OperationalError:
        uses_lists_column = False
        df_camps = fetch_data(
            f"SELECT id, opens FROM campaigns {where} ORDER BY id DESC LIMIT {lim}",
            params,
        )

    df_links = fetch_data(
        f"""
        SELECT l.campaign_id, l.clicks
        FROM   links l
        JOIN   (SELECT id FROM campaigns {where} ORDER BY id DESC LIMIT {lim}) c
               ON l.campaign_id = c.id
        WHERE  l.clicks IS NOT NULL AND l.clicks != ''
        """,
        params,
    )

    clicks_by_campaign: dict[int, set[int]] = {}
    for _, row in df_links.iterrows():
        cid = int(row["campaign_id"])
        s = row["clicks"] or ""
        if not s:
            continue
        acc = clicks_by_campaign.setdefault(cid, set())
        for sid_s in s.split(","):
            sid_s = sid_s.strip()
            if sid_s.isdigit():
                acc.add(int(sid_s))

    campaigns_out: list[dict] = []
    openers_total = 0
    for _, row in df_camps.iterrows():
        cid = int(row["id"])
        lids = (
            _parse_campaign_list_ids(row.get("lists"))
            if uses_lists_column
            else frozenset()
        )
        s = row["opens"] or ""
        openers: set[int] = set()
        if s:
            for entry in s.split(","):
                e = entry.strip()
                sid_s = e.split(":")[0] if ":" in e else e
                if sid_s.isdigit():
                    openers.add(int(sid_s))
        openers_total += len(openers)
        campaigns_out.append(
            {
                "id":       cid,
                "list_ids": lids,
                "openers":  openers,
                "clickers": clicks_by_campaign.get(cid, set()),
            }
        )

    has_list_targets = len(emails_sent_by_list) > 0 or any(
        len(c["list_ids"]) > 0 for c in campaigns_out
    )
    # If ``lists`` exists but never parses, do not filter opens/clicks by list
    scope_by_list = bool(uses_lists_column and has_list_targets)

    logger.info(
        "engagement_map: total_campaigns=%d parsed=%d uses_lists=%s "
        "list_buckets=%d scope_by_list=%s raw_open_ids=%d",
        total_campaigns,
        len(campaigns_out),
        uses_lists_column,
        len(emails_sent_by_list),
        scope_by_list,
        openers_total,
    )
    return {
        "emails_sent_by_list": emails_sent_by_list,
        "campaigns":           campaigns_out,
        "total_campaigns":     total_campaigns,
        "uses_lists_column":   uses_lists_column,
        "scope_by_list":       scope_by_list,
    }


def get_engagement_map_cached(*args, **kwargs) -> dict:
    # OPTIMIZED: request-scoped cache to prevent duplicate engagement map queries
    cache_key = repr((args, tuple(sorted(kwargs.items()))))
    try:
        store = getattr(g, "_engagement_map_request_cache", None)
        if store is None:
            store = {}
            g._engagement_map_request_cache = store
        if cache_key in store:
            return store[cache_key]
        out = get_engagement_map(*args, **kwargs)
        store[cache_key] = out
        return out
    except RuntimeError:
        return get_engagement_map(*args, **kwargs)


def get_queue_delivered_counts(
    sub_ids: list[int],
    date_from: str | None,
    date_to: str | None,
) -> dict[int, int] | None:
    """
    Per-subscriber **distinct campaigns** with ``queue.sent >= 1`` in the date
    range (Sendy ``queue`` + ``campaigns``).  Fast vs parsing ``opens`` longtext.

    Returns ``None`` if the ``queue`` table is missing or the query fails.
    """
    if not sub_ids:
        return {}
    clause, base_params = build_campaign_where_sent_in_range("c", date_from, date_to, {})
    # OPTIMIZED: chunk subscriber IN-list (same cap as list-id lookups; remote MySQL packet limits).
    _qc = USERS_EMAIL_IN_CHUNK if USERS_EMAIL_IN_CHUNK > 0 else 500
    _qc = min(_qc, 500)
    out: dict[int, int] = {}
    try:
        for _off in range(0, len(sub_ids), _qc):
            part = sub_ids[_off : _off + _qc]
            params = dict(base_params)
            placeholders = ", ".join(f":_q{i}" for i in range(len(part)))
            for i, sid in enumerate(part):
                params[f"_q{i}"] = int(sid)
            sql = f"""
                SELECT q.subscriber_id AS subscriber_id,
                       COUNT(DISTINCT q.campaign_id) AS n
                FROM   queue q
                INNER JOIN campaigns c ON c.id = q.campaign_id
                WHERE  q.subscriber_id IN ({placeholders})
                  AND  q.sent >= 1
                  AND  {clause}
                GROUP BY q.subscriber_id
            """
            df = fetch_data(sql, params)
            for _, row in df.iterrows():
                out[int(row["subscriber_id"])] = int(row["n"] or 0)
    except (OperationalError, SQLAlchemyError) as exc:
        logger.info("queue-based engagement unavailable (%s) — using lists/recipients logic", exc)
        return None
    return out


def build_engagement_for_subscribers(
    sub_ids: list[int],
    date_from: str | None = None,
    date_to: str | None = None,
) -> tuple[dict[str, dict], dict]:
    """
    Build per-subscriber engagement rows for API/JSON.

    Returns
    -------
    data   — ``{ "12345": { "emails_sent", "opens", "clicks", "open_pct", "click_pct", "checked" } }``
    meta   — ``total_campaigns``, ``uses_lists_column``, ``scope_by_list``, ``checked_campaigns``
    """
    if not sub_ids:
        return {}, {
            "total_campaigns": 0,
            "uses_lists_column": True,
            "scope_by_list": False,
            "checked_campaigns": 0,
        }

    bundle = get_engagement_map_cached(date_from=date_from, date_to=date_to)
    # OPTIMIZED: chunked IN-list to prevent oversized single query
    id2list: dict[int, int] = {}
    _list_id_chunk = USERS_EMAIL_IN_CHUNK if USERS_EMAIL_IN_CHUNK > 0 else 500
    _list_id_chunk = min(_list_id_chunk, 500)
    for _i in range(0, len(sub_ids), _list_id_chunk):
        id2list.update(get_subscriber_list_ids(sub_ids[_i : _i + _list_id_chunk]))
    by_list = bundle["emails_sent_by_list"]
    camps = bundle["campaigns"]
    total_c = int(bundle["total_campaigns"])
    use_lists = bool(bundle["uses_lists_column"])
    scope_by_list = bool(bundle.get("scope_by_list", False))
    queue_counts = get_queue_delivered_counts(sub_ids, date_from, date_to)

    result: dict[str, dict] = {}
    for sid in sub_ids:
        list_id = id2list.get(sid)
        if list_id is None:
            result[str(sid)] = {
                "emails_sent": 0,
                "opens": 0,
                "clicks": 0,
                "open_pct": 0.0,
                "click_pct": 0.0,
                "checked": 0,
            }
            continue

        if queue_counts is not None:
            emails_sent = int(queue_counts.get(sid, 0))
        elif scope_by_list and by_list:
            emails_sent = int(by_list.get(list_id, 0))
        else:
            emails_sent = total_c

        checked = 0
        opens_n = 0
        clicks_n = 0
        for camp in camps:
            lids = camp["list_ids"]
            # If we know which lists a campaign targeted, scope opens/clicks to
            # subscribers on those lists. If ``lists`` is empty/unknown, still
            # count the campaign — otherwise recent sends with blank ``lists``
            # yield checked=0 for everyone (common in some Sendy DBs).
            if scope_by_list and lids and list_id not in lids:
                continue
            checked += 1
            if sid in camp["openers"]:
                opens_n += 1
            if sid in camp["clickers"]:
                clicks_n += 1

        result[str(sid)] = {
            "emails_sent": emails_sent,
            "opens": opens_n,
            "clicks": clicks_n,
            "open_pct": round(opens_n * 100 / checked, 1) if checked else 0.0,
            "click_pct": round(clicks_n * 100 / checked, 1) if checked else 0.0,
            "checked": checked,
        }

    meta = {
        "total_campaigns": total_c,
        "uses_lists_column": use_lists,
        "scope_by_list": scope_by_list,
        "checked_campaigns": len(camps),
        "queue_counts_used": queue_counts is not None,
    }
    return result, meta


def aggregate_engagement_by_email_rows(
    records: list[dict],
    per_id_engagement: dict[str, dict],
) -> dict[str, dict]:
    """
    For rows grouped by email, sum ``emails_sent`` / ``opens`` / ``clicks`` across
    all ``subscriber_ids`` and set ``open_pct`` / ``click_pct`` vs total
    ``emails_sent``.  Mutates each record's ``id`` to ``min(subscriber_ids)``.
    """
    out: dict[str, dict] = {}
    for r in records:
        raw_ids = r.get("subscriber_ids")
        if isinstance(raw_ids, list) and raw_ids:
            ids = [int(x) for x in raw_ids]
        else:
            ids = [int(r["id"])]
        rep = min(ids)
        r["id"] = rep
        ms = mo = mc = ch = 0
        for i in ids:
            e = per_id_engagement.get(str(i), {})
            ms += int(e.get("emails_sent") or 0)
            mo += int(e.get("opens") or 0)
            mc += int(e.get("clicks") or 0)
            ch += int(e.get("checked") or 0)
        op = round(100.0 * mo / ms, 1) if ms else 0.0
        cp = round(100.0 * mc / ms, 1) if ms else 0.0
        out[str(rep)] = {
            "emails_sent": ms,
            "opens": mo,
            "clicks": mc,
            "open_pct": op,
            "click_pct": cp,
            "checked": ch,
        }
    return out


@cached(ttl=180)  # OPTIMIZED: increased TTL to reduce HOT query frequency
def get_campaigns_sent_in_range_list(
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """
    Sent campaigns in the date range — **campaigns table only** (no ``opens`` /
    ``links`` parsing).  Safe for large ``campaign_limit`` on slow remote MySQL.

    Rows match the shape expected by the dashboard campaign list + ``data`` array
    (zeros for metrics the UI can ignore).
    """
    clause, bind = build_campaign_where_sent_in_range("c", date_from, date_to, {})
    try:
        lim = CAMPAIGN_PERF_DEFAULT if limit is None else int(limit)
    except (TypeError, ValueError):
        lim = CAMPAIGN_PERF_DEFAULT
    lim = max(1, min(CAMPAIGN_PERF_MAX, lim))
    bind["_limit"] = lim
    _ts = campaign_effective_unix_ts_sql("c")
    sql = f"""
        SELECT
            c.id                                                            AS campaign_id,
            c.title,
            c.label                                                         AS subject,
            {_CAMPAIGN_DISPLAY_SQL}                                         AS campaign_display,
            c.send_date                                                     AS send_date,
            c.sent                                                          AS campaign_sent,
            c.recipients                                                    AS emails_sent,
            0                                                               AS total_opens,
            0                                                               AS total_clicks,
            0.0                                                             AS open_rate_pct,
            0.0                                                             AS click_rate_pct
        FROM campaigns c
        WHERE  {clause}
        ORDER  BY COALESCE({_ts}, 0) DESC, c.id DESC
        LIMIT  :_limit
    """
    return fetch_data(sql, bind)


def get_campaign_engagement_summary(
    date_from: str | None = None,
    date_to: str | None = None,
    status: str | None = None,
    email_totals: pd.DataFrame | None = None,
) -> dict[str, float | int | None | bool]:
    """
    Recipient-weighted open / click % over campaigns in the date range.

    When more than ``CAMPAIGN_ENGAGEMENT_SUMMARY_MAX`` (default 180) sends exist,
    we take a **middle** slice in send-time order (not always the newest 180),
    so nudging the date range usually changes which campaigns are included.
    The UI shows *N of M campaigns* when capped.

    Pass *email_totals* from the caller when ``get_email_totals`` was already
    executed for the same date range (avoids a duplicate query).
    """
    if email_totals is None:
        totals = get_email_totals(date_from=date_from, date_to=date_to)
    else:
        totals = email_totals
    total_c = int(totals.iloc[0]["campaigns_sent"]) if len(totals) else 0
    if total_c <= 0:
        return {
            "open_rate_pct": None,
            "click_rate_pct": None,
            "sample_campaigns": 0,
            "campaigns_in_range": 0,
            "campaigns_included": 0,
            "full_coverage": True,
            "sample_mode": "none",
            "sample_offset_rows": 0,
            "query_date_from": date_from,
            "query_date_to": date_to,
        }
    cap = CAMPAIGN_ENGAGEMENT_SUMMARY_MAX
    lim = max(1, min(cap, total_c))
    # When we subsample, **newest-only** rows barely change if the date window
    # still contains the same latest sends — rates look "stuck".  Use a
    # **middle** window in send-time order so shifting ``date_from`` / ``date_to``
    # changes which campaigns are included.
    offset_rows = max(0, (total_c - lim) // 2) if total_c > lim else 0
    sample_mode = "middle_window" if offset_rows > 0 else "all_in_range"

    df = get_campaign_performance(
        date_from=date_from,
        date_to=date_to,
        status=status,
        limit=lim,
        offset_rows=offset_rows,
    )
    if df is None or len(df) == 0:
        return {
            "open_rate_pct": None,
            "click_rate_pct": None,
            "sample_campaigns": 0,
            "campaigns_in_range": total_c,
            "campaigns_included": 0,
            "full_coverage": False,
            "sample_mode": sample_mode,
            "sample_offset_rows": offset_rows,
            "query_date_from": date_from,
            "query_date_to": date_to,
        }
    rec = pd.to_numeric(df["emails_sent"], errors="coerce").fillna(0.0)
    opens = pd.to_numeric(df["total_opens"], errors="coerce").fillna(0.0)
    clicks = pd.to_numeric(df["total_clicks"], errors="coerce").fillna(0.0)
    rsum = float(rec.sum())
    n = int(len(df))
    full = n >= total_c
    base_meta = {
        "sample_mode": sample_mode,
        "sample_offset_rows": offset_rows,
        "query_date_from": date_from,
        "query_date_to": date_to,
    }
    if rsum <= 0:
        return {
            "open_rate_pct": 0.0,
            "click_rate_pct": 0.0,
            "sample_campaigns": n,
            "campaigns_in_range": total_c,
            "campaigns_included": n,
            "full_coverage": full,
            **base_meta,
        }
    return {
        "open_rate_pct": round(float(opens.sum()) * 100.0 / rsum, 1),
        "click_rate_pct": round(float(clicks.sum()) * 100.0 / rsum, 1),
        "sample_campaigns": n,
        "campaigns_in_range": total_c,
        "campaigns_included": n,
        "full_coverage": full,
        **base_meta,
    }


def get_campaign_metrics_by_id(campaign_id: int) -> pd.DataFrame:
    """
    One-row Sendy-style metrics for a single campaign (dashboard expand-on-click).

    Opens/clicks from ``campaigns.opens`` / ``links.clicks`` via ``_comma_count``;
    bounces / unsubs / complaints / soft bounces from ``subscribers.last_campaign``.
    Not cached — reflects current DB when the user expands a row.
    """
    if not isinstance(campaign_id, int) or campaign_id <= 0:
        raise ValueError(f"campaign_id must be a positive integer, got {campaign_id!r}")

    opens_expr = _comma_count("c.opens")
    clicks_expr = _comma_count("lk.clicks")
    bind: dict = {"campaign_id": campaign_id}

    sql = f"""
        SELECT
            c.id                                                            AS campaign_id,
            c.title,
            c.label                                                         AS subject,
            {_CAMPAIGN_DISPLAY_SQL}                                         AS campaign_display,
            c.send_date                                                     AS send_date,
            c.sent                                                          AS campaign_sent,
            COALESCE(c.recipients, 0)                                       AS sent,
            COALESCE({opens_expr},  0)                                      AS opens,
            COALESCE(lnk_agg.total_clicks, 0)                                AS clicks,
            COALESCE(sub_agg.bounces, 0)                                    AS bounces,
            COALESCE(sub_agg.soft_bounces, 0)                               AS soft_bounces,
            COALESCE(sub_agg.unsubs, 0)                                     AS unsubscribes,
            COALESCE(sub_agg.complaints, 0)                                 AS complaints,
            ROUND({opens_expr} * 100.0 / NULLIF(c.recipients, 0), 2)       AS open_rate_pct,
            ROUND(COALESCE(lnk_agg.total_clicks, 0) * 100.0
                  / NULLIF(c.recipients, 0), 2)                             AS click_rate_pct,
            ROUND(COALESCE(sub_agg.bounces, 0) * 100.0
                  / NULLIF(c.recipients, 0), 2)                             AS bounce_rate_pct,
            ROUND(COALESCE(sub_agg.unsubs, 0) * 100.0
                  / NULLIF(c.recipients, 0), 2)                             AS unsub_rate_pct
        FROM campaigns c
        LEFT JOIN (
            SELECT lk.campaign_id,
                   SUM({clicks_expr}) AS total_clicks
            FROM   links lk
            WHERE  lk.campaign_id = :campaign_id
            GROUP  BY lk.campaign_id
        ) lnk_agg ON lnk_agg.campaign_id = c.id
        LEFT JOIN (
            SELECT s.last_campaign,
                   SUM(s.bounced)      AS bounces,
                   SUM(s.bounce_soft)  AS soft_bounces,
                   SUM(s.unsubscribed) AS unsubs,
                   SUM(s.complaint)    AS complaints
            FROM   subscribers s
            WHERE  s.last_campaign = :campaign_id
            GROUP  BY s.last_campaign
        ) sub_agg ON sub_agg.last_campaign = c.id
        WHERE  c.id = :campaign_id
    """
    return fetch_data(sql, bind)


def get_campaign_performance(
    date_from: str | None = None,
    date_to: str | None = None,
    status: str | None = None,
    limit: int | None = None,
    offset_rows: int = 0,
) -> pd.DataFrame:
    """
    Per-campaign engagement metrics (**not** application-cached — must track
    dashboard date-range changes immediately).

    Uses **two round-trips**: (1) cheap ``campaigns``-only id list with
    ``LIMIT``/``OFFSET``; (2) metrics for those ids via ``_comma_count`` on
    ``opens`` / ``clicks`` longtext.
    The old single-query shape duplicated the heavy date predicate on ``c2`` and
    timed out remote MySQL on large ``OFFSET`` (engagement summary middle window).

    *limit* / *offset_rows* — same semantics as before (newest first by send time).
    """
    opens_expr = _comma_count("c.opens")
    clicks_expr = _comma_count("lk.clicks")

    clause_c, bind = build_campaign_where_sent_in_range("c", date_from, date_to, {})
    try:
        lim = CAMPAIGN_PERF_DEFAULT if limit is None else int(limit)
    except (TypeError, ValueError):
        lim = CAMPAIGN_PERF_DEFAULT
    lim = max(1, min(CAMPAIGN_PERF_MAX, lim))
    try:
        off = max(0, int(offset_rows))
    except (TypeError, ValueError):
        off = 0
    bind["_limit"] = lim
    bind["_offset"] = off
    _ts = campaign_effective_unix_ts_sql("c")

    id_sql = f"""
        SELECT c.id AS campaign_id
        FROM   campaigns c
        WHERE  {clause_c}
        ORDER  BY COALESCE({_ts}, 0) DESC, c.id DESC
        LIMIT  :_limit OFFSET :_offset
    """
    df_ids = fetch_data(id_sql, bind)
    _perf_cols = [
        "campaign_id",
        "title",
        "subject",
        "campaign_display",
        "emails_sent",
        "total_opens",
        "total_clicks",
        "open_rate_pct",
        "click_rate_pct",
    ]
    if df_ids is None or len(df_ids) == 0:
        return pd.DataFrame(columns=_perf_cols)

    ids = [int(x) for x in df_ids["campaign_id"].tolist() if pd.notna(x)]
    if not ids:
        return pd.DataFrame(columns=_perf_cols)

    cchunk = CAMPAIGN_PERF_DETAIL_CHUNK
    detail_frames: list[pd.DataFrame] = []
    for c0 in range(0, len(ids), cchunk):
        sub = ids[c0 : c0 + cchunk]
        id_csv = ",".join(str(i) for i in sub)
        field_list = ",".join(str(i) for i in sub)
        detail_sql = f"""
            SELECT
                c.id                                                            AS campaign_id,
                c.title,
                c.label                                                         AS subject,
                {_CAMPAIGN_DISPLAY_SQL}                                         AS campaign_display,
                c.recipients                                                    AS emails_sent,
                COALESCE({opens_expr},  0)                                      AS total_opens,
                COALESCE(lnk.total_clicks, 0)                                   AS total_clicks,
                ROUND({opens_expr} * 100.0 / NULLIF(c.recipients, 0), 2)       AS open_rate_pct,
                ROUND(COALESCE(lnk.total_clicks, 0) * 100.0
                      / NULLIF(c.recipients, 0), 2)                             AS click_rate_pct
            FROM campaigns c
            LEFT JOIN (
                SELECT lk.campaign_id,
                       SUM({clicks_expr}) AS total_clicks
                FROM   links lk
                WHERE  lk.campaign_id IN ({id_csv})
                GROUP  BY lk.campaign_id
            ) lnk ON lnk.campaign_id = c.id
            WHERE  c.id IN ({id_csv})
            ORDER  BY FIELD(c.id, {field_list})
        """
        detail_frames.append(fetch_data(detail_sql, {}))

    if not detail_frames:
        return pd.DataFrame(columns=_perf_cols)
    return pd.concat(detail_frames, ignore_index=True)
