"""
Sendy-specific database queries — built from the ACTUAL schema
revealed by show_schema.py.

Schema facts (verified)
-----------------------
campaigns
  sent          varchar   '' / '0' / NULL = not sent; anything else = sent (Unix ts or '1')
  send_date     varchar   Unix timestamp string (10 digits) OR 'YYYY-MM-DD HH:MM:SS'
  recipients    int       total addresses delivered to (NOT total_recipients)
  opens         longtext  PHP serialized array — a:N:{i:0;i:SUB_ID;…}  → count = N
  label         varchar   campaign label / subject line  (no 'subject' column)
  title         varchar   internal campaign name
  app           int       brand/app ID

links
  campaign_id   int       → campaigns.id
  clicks        longtext  PHP serialized array per link (same format as opens)

subscribers
  list          int       list ID  (NOT 'l')
  confirmed     int       1 = confirmed  (NOT 'y'/'n'; default is 1)
  timestamp     int       Unix timestamp of sign-up  (NOT 'created')
  unsubscribed  int       1 = unsubscribed
  bounced       int       1 = hard bounce
  bounce_soft   int       1 = soft bounce
  complaint     int       1 = spam complaint
  last_campaign int       last campaign sent to this subscriber  (indexed → fast GROUP BY)

lists
  app           int       brand/app ID  (NOT brand_id)
  name          varchar   list name

Derived metrics (no direct columns in campaigns)
  clicks:       SUM of parsed link click counts from links.clicks per campaign
  bounces:      COUNT of subscribers.bounced=1 grouped by last_campaign
  unsubscribes: COUNT of subscribers.unsubscribed=1 grouped by last_campaign
"""

import logging

import pandas as pd

from cache import cached
from db import fetch_data
from queries import (
    _CAMPAIGN_DISPLAY_SQL,
    build_campaign_where_sent_in_range,
    campaign_effective_unix_ts_sql,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL building-block helpers
# ---------------------------------------------------------------------------

def _comma_count(col: str) -> str:
    """
    SQL expression: count comma-separated entries in *col*.

    Sendy stores opens as 'sub_id:country,sub_id:country,...' and clicks as
    'sub_id,sub_id,...' — both are comma-delimited, NOT PHP serialized.

    Formula: (string length) − (length with commas removed) + 1
    Returns 0 for NULL / empty string.
    """
    return (
        f"CASE WHEN {col} IS NULL OR {col} = '' THEN 0 "
        f"ELSE LENGTH({col}) - LENGTH(REPLACE({col}, ',', '')) + 1 END"
    )

# Keep old name as alias so any leftover references don't break
_php_count = _comma_count


# Plain string (no f-prefix) so {9,11} is a literal regex quantifier, not an f-string brace.
# Converts send_date varchar to a comparable DATETIME value.
_SEND_DT = (
    "CASE "
    "WHEN send_date REGEXP '^[0-9]{9,11}$' "
    "  THEN FROM_UNIXTIME(CAST(send_date AS UNSIGNED)) "
    "WHEN send_date LIKE '____-__-__%' "
    "  THEN STR_TO_DATE(LEFT(send_date, 10), '%Y-%m-%d') "
    "ELSE NULL END"
)

_PERIOD_FMT: dict[str, str] = {
    "day":   "%Y-%m-%d",
    "month": "%Y-%m",
}


def _campaign_where(
    date_from: str | None,
    date_to: str | None,
    bind: dict | None = None,
) -> tuple[str, dict]:
    """``WHERE`` clause body for sent campaigns in a calendar date range."""
    clause, p = build_campaign_where_sent_in_range("c", date_from, date_to, bind or {})
    return clause, p


def _period_expr(fmt: str) -> str:
    """
    SQL expression: format the campaign send timestamp as a period string.

    Uses the same effective-time logic as ``build_campaign_where_sent_in_range``
    (``sent`` may be ``'1'``; then ``send_date`` is used — real Sendy rows).
    """
    ts = campaign_effective_unix_ts_sql("c")
    return f"DATE_FORMAT(FROM_UNIXTIME({ts}), '{fmt}')"


# ---------------------------------------------------------------------------
# Campaign stats
# ---------------------------------------------------------------------------

@cached(ttl=300)
def get_overview_stats(
    date_from: str | None = None,
    date_to: str | None = None,
) -> pd.DataFrame:
    """
    Single-row aggregate across all sent campaigns in the date range.

    A date range MUST be provided (or defaults to the last 90 days) because
    computing LENGTH(REPLACE(opens,...)) on thousands of longtext rows without
    a filter can exhaust the server's 60-second query timeout.

    Returns columns:
        total_campaigns, total_sent, total_opens, total_clicks,
        total_bounces, total_unsubscribes, total_complaints,
        avg_open_rate_pct, avg_click_rate_pct, avg_bounce_rate_pct.
    """
    import datetime as _dt
    if date_from is None:
        date_from = (_dt.date.today() - _dt.timedelta(days=90)).isoformat()
    if date_to is None:
        date_to = _dt.date.today().isoformat()

    bind = {}
    cw, bind = _campaign_where(date_from, date_to, bind)
    where_sql    = "WHERE " + cw

    clicks_expr = _comma_count("lk.clicks")

    # ── Query 1 (~0.4 s): campaign count, total sent, total clicks ────────
    # Clicks use a pre-aggregated JOIN on links (10 K rows — always fast).
    # Opens are NOT computed here because LENGTH(REPLACE(opens,...)) on 300+
    # longtext rows can take 15–60 s and time out on shared hosting.
    sql_main = f"""
        SELECT
            COUNT(c.id)                       AS total_campaigns,
            COALESCE(SUM(c.recipients), 0)    AS total_sent,
            COALESCE(SUM(lnk_agg.clicks), 0)  AS total_clicks
        FROM campaigns c
        LEFT JOIN (
            SELECT lk.campaign_id,
                   SUM({clicks_expr}) AS clicks
            FROM   links lk
            GROUP  BY lk.campaign_id
        ) lnk_agg ON lnk_agg.campaign_id = c.id
        {where_sql}
    """
    df_main = fetch_data(sql_main, bind)

    # ── Query 2 (~7 s): subscriber stats filtered to campaigns in range ───
    # Running 3 subqueries in a single round-trip is faster than 3 separate
    # network calls.  COUNT(*) is faster than SUM() for these filters.
    sql_subs = f"""
        SELECT
            (SELECT COUNT(*) FROM subscribers
             WHERE  bounced = 1
               AND  last_campaign IN (SELECT c.id FROM campaigns c {where_sql})
            ) AS total_bounces,
            (SELECT COUNT(*) FROM subscribers
             WHERE  unsubscribed = 1
               AND  last_campaign IN (SELECT c.id FROM campaigns c {where_sql})
            ) AS total_unsubscribes,
            (SELECT COUNT(*) FROM subscribers) AS total_subscribers
    """
    df_subs = fetch_data(sql_subs, bind)

    # ── Merge ─────────────────────────────────────────────────────────────
    total_sent    = int(df_main.iloc[0]["total_sent"]    or 0) if len(df_main) else 0
    total_clicks  = int(df_main.iloc[0]["total_clicks"]  or 0) if len(df_main) else 0
    total_bounces = int(df_subs.iloc[0]["total_bounces"] or 0) if len(df_subs) else 0

    merged = {
        "total_campaigns":    int(df_main.iloc[0]["total_campaigns"] or 0) if len(df_main) else 0,
        "total_sent":         total_sent,
        "total_clicks":       total_clicks,
        "total_bounces":      total_bounces,
        "total_unsubscribes": int(df_subs.iloc[0]["total_unsubscribes"] or 0) if len(df_subs) else 0,
        "total_subscribers":  int(df_subs.iloc[0]["total_subscribers"]  or 0) if len(df_subs) else 0,
        # avg_open_rate computed separately via /api/sendy/stats?type=open_rate
        # to avoid timing out on the opens longtext column
        "avg_open_rate_pct":  None,
        "avg_click_rate_pct": round(total_clicks * 100.0 / total_sent, 2) if total_sent else 0,
        "avg_bounce_rate_pct": round(total_bounces * 100.0 / total_sent, 2) if total_sent else 0,
    }
    return pd.DataFrame([merged])


@cached(ttl=3600)   # 1 hour — this query takes 15–20 s so cache aggressively
def get_avg_open_rate(
    date_from: str | None = None,
    date_to: str | None = None,
) -> float:
    """
    Compute the weighted average open rate across all sent campaigns in the range.

    This is SLOW (15–20 s) because it runs LENGTH(REPLACE(opens,...)) on
    potentially hundreds of longtext fields.  It is cached for 1 hour and
    loaded asynchronously by the frontend so it does not block the initial page.
    """
    import datetime as _dt
    if date_from is None:
        date_from = (_dt.date.today() - _dt.timedelta(days=90)).isoformat()
    if date_to is None:
        date_to = _dt.date.today().isoformat()

    bind = {}
    cw, bind = _campaign_where(date_from, date_to, bind)
    where_sql    = "WHERE " + cw
    opens_expr   = _comma_count("c.opens")

    sql = f"""
        SELECT
            COALESCE(SUM({opens_expr}), 0)              AS total_opens,
            COALESCE(SUM(c.recipients),   0)              AS total_sent,
            ROUND(
                COALESCE(SUM({opens_expr}), 0) * 100.0
                / NULLIF(SUM(c.recipients), 0), 2
            )                                           AS avg_open_rate_pct
        FROM campaigns c
        {where_sql}
    """
    df = fetch_data(sql, bind)
    if not len(df):
        return 0.0
    return {
        "total_opens":       int(df.iloc[0]["total_opens"]      or 0),
        "avg_open_rate_pct": float(df.iloc[0]["avg_open_rate_pct"] or 0),
    }


@cached(ttl=300)
def get_emails_over_time(
    date_from: str | None = None,
    date_to: str | None = None,
    group_by: str = "month",
) -> pd.DataFrame:
    """
    Emails sent and clicks aggregated by time period (fast: < 1 s).

    Opens and bounces are excluded here because they require expensive
    LENGTH(REPLACE(opens,...)) or full subscriber-table operations that
    can take 15–35 s. Opens rate is loaded separately via
    ``get_avg_open_rate()`` (async, cached 1 h) and per-campaign opens are
    shown in the campaign performance table from ``get_campaigns_stats()``.
    """
    fmt        = _PERIOD_FMT.get(group_by, "%Y-%m")
    period_col = _period_expr(fmt)
    clicks_expr = _comma_count("lk.clicks")

    bind = {}
    cw, bind = _campaign_where(date_from, date_to, bind)
    where_sql    = "WHERE " + cw

    sql = f"""
        SELECT
            {period_col}                                AS period,
            COUNT(DISTINCT c.id)                        AS campaigns,
            COALESCE(SUM(c.recipients), 0)              AS emails_sent,
            COALESCE(SUM(lnk_agg.clicks), 0)            AS clicks,
            ROUND(COALESCE(SUM(lnk_agg.clicks), 0) * 100.0
                  / NULLIF(SUM(c.recipients), 0), 2)    AS click_rate_pct
        FROM campaigns c
        LEFT JOIN (
            SELECT lk.campaign_id,
                   SUM({clicks_expr}) AS clicks
            FROM   links lk
            GROUP  BY lk.campaign_id
        ) lnk_agg ON lnk_agg.campaign_id = c.id
        {where_sql}
        GROUP  BY period
        HAVING period IS NOT NULL
        ORDER  BY period ASC
    """
    return fetch_data(sql, bind)


@cached(ttl=120)
def count_sent_campaigns(
    date_from: str | None = None,
    date_to: str | None = None,
) -> int:
    """Total number of sent campaigns matching the date filter."""
    bind = {}
    cw, bind = _campaign_where(date_from, date_to, bind)
    where_sql = "WHERE " + cw

    df = fetch_data(f"SELECT COUNT(*) AS total FROM campaigns c {where_sql}", bind)
    return int(df.iloc[0]["total"]) if len(df) else 0


@cached(ttl=300)
def get_campaigns_stats(
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> pd.DataFrame:
    """
    Per-campaign breakdown, newest first.

    Opens come from parsing the PHP-serialized ``campaigns.opens`` longtext.
    Clicks are summed from ``links.clicks`` per campaign (correlated subquery,
    efficient because LIMIT keeps the outer result small).
    Bounces / unsubscribes are attributed via ``subscribers.last_campaign``.
    """
    bind = {}
    cw, bind = _campaign_where(date_from, date_to, bind)
    where_sql = "WHERE " + cw
    bind["_limit"]  = limit
    bind["_offset"] = offset

    opens_expr  = _comma_count("c.opens")
    clicks_expr = _comma_count("lk.clicks")   # alias must match FROM links lk
    period_col  = _period_expr("%Y-%m-%d")

    # Pre-aggregate clicks (links: 10K rows) and subscriber stats (11.9M rows,
    # indexed on last_campaign) into derived tables — avoids N correlated
    # subqueries per campaign row which would take 8+ seconds for 200 rows.
    sql = f"""
        SELECT
            c.id                                                            AS campaign_id,
            c.title,
            c.label                                                         AS subject,
            {_CAMPAIGN_DISPLAY_SQL}                                         AS campaign_display,
            {period_col}                                                    AS send_date,
            COALESCE(c.recipients, 0)                                       AS sent,
            COALESCE({opens_expr},  0)                                      AS opens,
            COALESCE(lnk_agg.clicks, 0)                                     AS clicks,
            COALESCE(sub_agg.bounces, 0)                                    AS bounces,
            COALESCE(sub_agg.unsubs,  0)                                    AS unsubscribes,
            ROUND({opens_expr} * 100.0 / NULLIF(c.recipients, 0), 2)       AS open_rate_pct,
            ROUND(COALESCE(lnk_agg.clicks, 0) * 100.0
                  / NULLIF(c.recipients, 0), 2)                             AS click_rate_pct,
            ROUND(COALESCE(sub_agg.bounces, 0) * 100.0
                  / NULLIF(c.recipients, 0), 2)                             AS bounce_rate_pct
        FROM campaigns c
        -- clicks: pre-aggregate links (10 K rows, fast)
        LEFT JOIN (
            SELECT lk.campaign_id,
                   SUM({clicks_expr}) AS clicks
            FROM   links lk
            GROUP  BY lk.campaign_id
        ) lnk_agg ON lnk_agg.campaign_id = c.id
        -- bounces/unsubscribes: filter to campaigns in the requested range only
        -- so we scan ~379×3K = ~1.2M rows instead of all 11.9M subscribers
        LEFT JOIN (
            SELECT s.last_campaign,
                   SUM(s.bounced)      AS bounces,
                   SUM(s.unsubscribed) AS unsubs
            FROM   subscribers s
            WHERE  s.last_campaign IN (
                SELECT c.id FROM campaigns c {where_sql}
            )
            GROUP  BY s.last_campaign
        ) sub_agg ON sub_agg.last_campaign = c.id
        {where_sql}
        ORDER  BY COALESCE({campaign_effective_unix_ts_sql("c")}, 0) DESC, c.id DESC
        LIMIT  :_limit OFFSET :_offset
    """
    return fetch_data(sql, bind)


# ---------------------------------------------------------------------------
# Subscriber stats
# ---------------------------------------------------------------------------

@cached(ttl=120)
def get_subscriber_totals() -> pd.DataFrame:
    """
    Single-row aggregate across all subscribers.

    subscribers.confirmed = 1  (int, NOT 'y')
    subscribers.list      = list ID  (NOT 'l')
    Active = confirmed=1 AND unsubscribed=0 AND bounced=0 AND complaint=0
    """
    sql = """
        SELECT
            COUNT(*)                                                     AS total,
            SUM(CASE WHEN confirmed    = 1
                      AND unsubscribed = 0
                      AND bounced      = 0
                      AND complaint    = 0 THEN 1 ELSE 0 END)           AS active,
            SUM(CASE WHEN confirmed = 1 THEN 1 ELSE 0 END)              AS confirmed,
            SUM(unsubscribed)                                            AS unsubscribed,
            SUM(bounced)                                                 AS bounced,
            SUM(bounce_soft)                                             AS bounce_soft,
            SUM(complaint)                                               AS complaints
        FROM subscribers
    """
    return fetch_data(sql, {})


@cached(ttl=120)
def get_subscriber_growth(
    date_from: str | None = None,
    date_to: str | None = None,
    group_by: str = "month",
) -> pd.DataFrame:
    """
    New subscriber sign-ups per period.

    subscribers.timestamp stores a Unix timestamp (int) — NOT a datetime column.
    Date filters are therefore converted with UNIX_TIMESTAMP().
    """
    fmt = _PERIOD_FMT.get(group_by, "%Y-%m")

    conditions: list[str] = []
    bind: dict = {}
    if date_from:
        conditions.append("timestamp >= UNIX_TIMESTAMP(:date_from)")
        bind["date_from"] = date_from
    if date_to:
        conditions.append("timestamp < UNIX_TIMESTAMP(DATE_ADD(:date_to, INTERVAL 1 DAY))")
        bind["date_to"] = date_to
    where_sql = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    sql = f"""
        SELECT
            DATE_FORMAT(FROM_UNIXTIME(timestamp), '{fmt}')               AS period,
            COUNT(*)                                                     AS new_subscribers,
            SUM(CASE WHEN unsubscribed = 0
                      AND bounced      = 0
                      AND complaint    = 0 THEN 1 ELSE 0 END)           AS still_active,
            SUM(unsubscribed)                                            AS unsubscribed,
            SUM(bounced)                                                 AS bounced
        FROM   subscribers
        {where_sql}
        GROUP  BY period
        ORDER  BY period ASC
    """
    return fetch_data(sql, bind)


@cached(ttl=120)
def get_lists_summary() -> pd.DataFrame:
    """
    Per-list subscriber health.

    subscribers.list = list ID  (single column name 'list', not 'l')
    """
    sql = """
        SELECT
            li.id                                                        AS list_id,
            li.name                                                      AS list_name,
            COUNT(s.id)                                                  AS total,
            SUM(CASE WHEN s.confirmed    = 1
                      AND s.unsubscribed = 0
                      AND s.bounced      = 0
                      AND s.complaint    = 0 THEN 1 ELSE 0 END)         AS active,
            SUM(s.unsubscribed)                                          AS unsubscribed,
            SUM(s.bounced + s.bounce_soft)                               AS bounced,
            SUM(s.complaint)                                             AS complaints,
            ROUND(
                SUM(s.bounced + s.bounce_soft) * 100.0
                / NULLIF(COUNT(s.id), 0), 2
            )                                                            AS bounce_rate_pct
        FROM   lists li
        LEFT   JOIN subscribers s ON s.list = li.id
        GROUP  BY li.id, li.name
        ORDER  BY total DESC
    """
    return fetch_data(sql, {})
