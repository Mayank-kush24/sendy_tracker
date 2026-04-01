"""
Flask application entry point for the sendy_tracker API.

Run in development:
    flask --app app run --debug

Run in production (example with gunicorn):
    gunicorn "app:create_app()" -w 4 -b 0.0.0.0:5000
"""

import functools
import hashlib
import json
import logging
import os
import time
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

import pandas as pd
from flask import (
    Flask,
    current_app,
    g,
    jsonify,
    make_response,
    render_template,
    request,
    Response,
)
from sqlalchemy.exc import OperationalError, SQLAlchemyError

import cache
import queries
import sendy_queries

# ---------------------------------------------------------------------------
# Pagination constants  (overridable via env vars)
# ---------------------------------------------------------------------------
_DEFAULT_LIMIT = int(os.getenv("DEFAULT_PAGE_LIMIT", "100"))
_MAX_LIMIT     = int(os.getenv("MAX_PAGE_LIMIT",     "1000"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


def _live_html(template_name: str) -> Response:
    """
    Serve Jinja HTML with strict ``no-store`` and **always** a full ``200`` body.

    We intentionally do **not** use ``ETag`` / ``make_conditional`` here: if the
    browser has an old cached document, a ``304 Not Modified`` can make it
    **reuse that stale HTML** while the new ``Cache-Control`` never applies to
    the cached entry — which looks like “the server never updates”.
    """
    app = current_app
    folder = app.template_folder or "templates"
    path = os.path.normpath(os.path.join(app.root_path, folder, template_name))
    try:
        mstamp = int(os.path.getmtime(path))
    except OSError:
        logger.error("Template not found: %s", path)
        mstamp = int(time.time())
    body = render_template(template_name, _template_mtime=mstamp)
    resp = make_response(body)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    resp.headers["X-Template-File-Mtime"] = str(mstamp)
    return resp


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _handle_db_errors(fn):
    """
    Decorator that catches DB and validation exceptions and converts them to
    appropriate JSON error responses so every route stays free of boilerplate.

    400  ValueError          – bad / missing query param
    503  OperationalError    – DB unreachable
    500  SQLAlchemyError     – any other DB error
    """
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except OperationalError as exc:
            logger.error("DB connection error in %s: %s", fn.__name__, exc)
            return (
                jsonify({"error": "Database unavailable", "detail": str(exc.orig)}),
                503,
            )
        except SQLAlchemyError as exc:
            logger.error("DB error in %s: %s", fn.__name__, exc)
            return jsonify({"error": "Database error", "detail": str(exc)}), 500
    return wrapper


def _clean_records(df: pd.DataFrame) -> list[dict]:
    """
    Convert a DataFrame to a list of dicts, dropping any key whose value is
    ``None`` or ``NaN``.  This keeps JSON payloads lean — callers never see
    ``"field": null`` for fields that simply don't apply to a row.
    """
    df = df.where(df.notna(), other=None)
    return [
        {k: v for k, v in record.items() if v is not None}
        for record in df.to_dict(orient="records")
    ]


def _parse_date(value: str, name: str) -> str:
    """Validate *value* is ``YYYY-MM-DD``; raise ``ValueError`` otherwise."""
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"'{name}' must be YYYY-MM-DD, got {value!r}")
    return value


def _parse_int(value: str, name: str) -> int:
    """Parse *value* as a positive integer; raise ``ValueError`` with a helpful message."""
    try:
        parsed = int(value)
    except (ValueError, TypeError):
        raise ValueError(f"'{name}' must be an integer, got {value!r}")
    if parsed <= 0:
        raise ValueError(f"'{name}' must be a positive integer, got {parsed}")
    return parsed


def _collect_date_filters(args) -> tuple[str | None, str | None]:
    """Parse and validate the common ``date_from`` / ``date_to`` query params."""
    date_from = _parse_date(args["date_from"], "date_from") if "date_from" in args else None
    date_to   = _parse_date(args["date_to"],   "date_to")   if "date_to"   in args else None
    return date_from, date_to


def _parse_pagination(args) -> tuple[int, int]:
    """
    Parse and validate ``limit`` and ``offset`` query parameters.

    Returns ``(limit, offset)``.

    Raises:
        ValueError: if either value is out of the allowed range.
    """
    raw_limit  = args.get("limit",  str(_DEFAULT_LIMIT))
    raw_offset = args.get("offset", "0")

    try:
        limit = int(raw_limit)
    except (ValueError, TypeError):
        raise ValueError(f"'limit' must be an integer, got {raw_limit!r}")
    if not 1 <= limit <= _MAX_LIMIT:
        raise ValueError(f"'limit' must be 1–{_MAX_LIMIT}, got {limit}")

    try:
        offset = int(raw_offset)
    except (ValueError, TypeError):
        raise ValueError(f"'offset' must be an integer, got {raw_offset!r}")
    if offset < 0:
        raise ValueError(f"'offset' must be >= 0, got {offset}")

    return limit, offset


def _pagination_meta(
    total: int | None,
    limit: int,
    offset: int,
    *,
    has_more: bool | None = None,
) -> dict:
    """Build the standard pagination envelope included in every list response.

    When *total* is ``None`` (expensive or unknown), pass *has_more* explicitly.
    """
    if total is not None:
        hm = offset + limit < total
        return {
            "total":       total,
            "limit":       limit,
            "offset":      offset,
            "has_more":    hm,
            "next_offset": offset + limit if hm else None,
        }
    if has_more is None:
        raise ValueError("has_more is required when total is None")
    return {
        "total":                         None,
        "total_distinct_emails_unknown": True,
        "limit":                         limit,
        "offset":                        offset,
        "has_more":                      has_more,
        "next_offset":                   offset + limit if has_more else None,
    }


def _etag_response(
    payload: dict,
    max_age: int = 60,
) -> Response:
    """
    Build a ``flask.Response`` with ``Cache-Control`` and ``ETag`` headers.

    If the request already carries a matching ``If-None-Match`` header the
    function returns HTTP 304 Not Modified immediately, saving bandwidth.

    Args:
        payload: The dict to serialise as JSON.
        max_age: ``max-age`` value for the ``Cache-Control`` header (seconds).
    """
    body = json.dumps(payload, default=str)
    etag = '"' + hashlib.md5(body.encode()).hexdigest() + '"'

    if request.headers.get("If-None-Match") == etag:
        return Response(status=304)

    resp = Response(body, status=200, mimetype="application/json")
    resp.headers["Cache-Control"] = f"max-age={max_age}, must-revalidate"
    resp.headers["ETag"]          = etag
    return resp


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app() -> Flask:
    app = Flask(__name__)

    app.config["JSON_SORT_KEYS"] = False

    # When FLASK_DEBUG=false, Flask normally caches Jinja templates until process
    # restart — HTML/JS edits then look "ignored". Reload templates on each request
    # unless opted out: FLASK_TEMPLATES_AUTO_RELOAD=false
    _tre = os.getenv("FLASK_TEMPLATES_AUTO_RELOAD", "true").lower()
    app.config["TEMPLATES_AUTO_RELOAD"] = _tre not in ("0", "false", "no", "off")

    cache_status = cache.init_cache()
    logger.info("Cache backend: %s", cache_status)
    tmpl_dir = os.path.abspath(os.path.join(app.root_path, app.template_folder or "templates"))
    logger.info(
        "Dashboard HTML from: %s | TEMPLATES_AUTO_RELOAD=%s",
        tmpl_dir,
        app.config.get("TEMPLATES_AUTO_RELOAD"),
    )

    @app.before_request
    def _start_timer():
        g.t0 = time.perf_counter()

    @app.after_request
    def _add_headers(response: Response) -> Response:
        elapsed_ms = (time.perf_counter() - g.t0) * 1000
        response.headers["X-Response-Time"] = f"{elapsed_ms:.1f}ms"
        response.headers["X-Content-Type-Options"] = "nosniff"
        # Browsers aggressively cache HTML; without this, dashboard edits look "stuck".
        try:
            path = request.path
        except RuntimeError:
            path = ""
        if path in ("/", "/dashboard", "/sendy"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

    _register_routes(app)
    _register_error_handlers(app)

    return app


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

def _register_routes(app: Flask) -> None:

    @app.get("/")
    @app.get("/dashboard")
    def dashboard():
        """Serve the generic analytics dashboard."""
        return _live_html("dashboard.html")

    @app.get("/sendy")
    def sendy_dashboard():
        """Serve the Sendy-specific central analytics dashboard."""
        return _live_html("sendy_dashboard.html")

    # ------------------------------------------------------------------
    # /api/lists  – mailing list id + name (for dashboard filters)
    # ------------------------------------------------------------------
    @app.get("/api/lists")
    @_handle_db_errors
    def mailing_lists():
        """Return all lists for populating the List filter dropdown."""
        df = queries.get_mailing_lists()
        return _etag_response(
            {"data": _clean_records(df), "count": len(df)},
            max_age=600,
        )

    # ------------------------------------------------------------------
    # /api/sendy/stats  – overview + time-series (single call for dashboard)
    # ------------------------------------------------------------------
    @app.get("/api/sendy/stats")
    @_handle_db_errors
    def sendy_stats():
        """
        Aggregate Sendy campaign stats + time-series in one response.

        Query parameters
        ----------------
        date_from : YYYY-MM-DD  – lower bound on send_date
        date_to   : YYYY-MM-DD  – upper bound on send_date
        group_by  : day|month   – time-series granularity (default: month)

        Response shape
        --------------
        200 {
              "overview":    { total_campaigns, total_sent, total_opens, … },
              "time_series": [ { period, emails_sent, opens, clicks, … }, … ],
              "subscribers": { total, active, confirmed, bounced, … },
              "filters":     { … }
            }
        """
        args      = request.args
        date_from, date_to = _collect_date_filters(args)
        group_by  = args.get("group_by", "month").lower().strip()
        if group_by not in ("day", "month"):
            raise ValueError("'group_by' must be 'day' or 'month'")

        stat_type = args.get("type", "overview").lower().strip()

        active_filters = {k: v for k, v in {
            "date_from": date_from, "date_to": date_to, "group_by": group_by,
        }.items() if v}

        # open_rate is slow (15–20 s) — loaded separately by the frontend
        if stat_type == "open_rate":
            result = sendy_queries.get_avg_open_rate(date_from=date_from, date_to=date_to)
            return _etag_response({
                "type":    "open_rate",
                "data":    result if isinstance(result, dict) else {"avg_open_rate_pct": 0, "total_opens": 0},
                "filters": active_filters,
            }, max_age=3600)

        overview    = sendy_queries.get_overview_stats(date_from=date_from, date_to=date_to)
        time_series = sendy_queries.get_emails_over_time(date_from=date_from, date_to=date_to, group_by=group_by)
        subscribers = sendy_queries.get_subscriber_totals()

        return _etag_response({
            "overview":    _clean_records(overview)[0]    if len(overview)    else {},
            "time_series": _clean_records(time_series),
            "subscribers": _clean_records(subscribers)[0] if len(subscribers) else {},
            "filters":     active_filters,
        }, max_age=300)

    # ------------------------------------------------------------------
    # /api/sendy/campaigns  – paginated per-campaign breakdown
    # ------------------------------------------------------------------
    @app.get("/api/sendy/campaigns")
    @_handle_db_errors
    def sendy_campaigns():
        """
        Paginated list of sent campaigns with individual stats.

        Query parameters
        ----------------
        date_from : YYYY-MM-DD
        date_to   : YYYY-MM-DD
        limit     : int  (1–MAX_PAGE_LIMIT, default 50)
        offset    : int  (default 0)
        """
        args = request.args
        date_from, date_to = _collect_date_filters(args)
        limit, offset      = _parse_pagination(args)

        total = sendy_queries.count_sent_campaigns(date_from=date_from, date_to=date_to)
        df    = sendy_queries.get_campaigns_stats(
            date_from=date_from, date_to=date_to,
            limit=limit, offset=offset,
        )

        active_filters = {k: v for k, v in {
            "date_from": date_from, "date_to": date_to,
        }.items() if v}

        return _etag_response({
            "data":       _clean_records(df),
            "pagination": _pagination_meta(total, limit, offset),
            "filters":    active_filters,
        }, max_age=300)

    # ------------------------------------------------------------------
    # /api/sendy/subscribers  – subscriber totals + growth + per-list
    # ------------------------------------------------------------------
    @app.get("/api/sendy/subscribers")
    @_handle_db_errors
    def sendy_subscribers():
        """
        Subscriber health metrics across all lists.

        Query parameters
        ----------------
        date_from : YYYY-MM-DD  – growth chart lower bound
        date_to   : YYYY-MM-DD  – growth chart upper bound
        group_by  : day|month   – growth granularity (default: month)
        """
        args     = request.args
        date_from, date_to = _collect_date_filters(args)
        group_by = args.get("group_by", "month").lower().strip()
        if group_by not in ("day", "month"):
            raise ValueError("'group_by' must be 'day' or 'month'")

        totals = sendy_queries.get_subscriber_totals()
        growth = sendy_queries.get_subscriber_growth(date_from=date_from, date_to=date_to, group_by=group_by)
        lists  = sendy_queries.get_lists_summary()

        return _etag_response({
            "totals": _clean_records(totals)[0] if len(totals) else {},
            "growth": _clean_records(growth),
            "lists":  _clean_records(lists),
        }, max_age=120)

    @app.get("/health")
    def health():
        """Lightweight liveness probe – no DB call."""
        return jsonify({"status": "ok"}), 200

    # ------------------------------------------------------------------
    # /api/data  – original generic endpoint (kept for compatibility)
    # ------------------------------------------------------------------
    @app.get("/api/data")
    @_handle_db_errors
    def get_data():
        """
        Return tabular data as JSON.

        Query parameters
        ----------------
        resource : str  (default: "subscribers")
            One of:  subscribers | campaigns | campaign_stats | active_counts
        list_id  : int  (optional, used when resource=subscribers)
        campaign_id : int  (optional, used when resource=campaigns)
        """
        resource = request.args.get("resource", "subscribers").lower().strip()
        df = _dispatch(resource, request.args)
        return jsonify(
            {
                "resource": resource,
                "count": len(df),
                "data": _clean_records(df),
            }
        ), 200

    # ------------------------------------------------------------------
    # /api/users   (paginated)
    # ------------------------------------------------------------------
    @app.get("/api/users")
    @_handle_db_errors
    def get_users():
        """
        Return a paginated page of **distinct emails** (merged across lists).

        Query parameters
        ----------------
        date_from : YYYY-MM-DD  – lower bound on sign-up date (inclusive)
        date_to   : YYYY-MM-DD  – upper bound on sign-up date (inclusive)
        list_id   : int         – restrict to a single mailing list
        status    : str         – match the ``status`` column exactly
        q / search: str         – optional substring match on email (case-insensitive)
        limit     : int         – rows per page (1–MAX_PAGE_LIMIT, default 100)
        offset    : int         – rows to skip (default 0)

        Each row includes ``subscriber_ids`` (all Sendy subscriber rows for that
        email).  ``pagination.total`` is omitted (``null``) — exact distinct-email
        counts are not run on large DBs; use ``pagination.has_more`` for paging.

        Response shape
        --------------
        200 {
              "data":       [ { <no null fields> }, ... ],
              "pagination": { "total", "limit", "offset", "has_more",
                              "next_offset" },
              "filters":    { <only active filters> }
            }
        304  Not Modified  (when ETag matches If-None-Match header)
        """
        args = request.args
        date_from, date_to = _collect_date_filters(args)
        list_id = _parse_int(args["list_id"], "list_id") if "list_id" in args else None
        status  = args.get("status") or None
        limit, offset = _parse_pagination(args)
        include_engagement = request.args.get("include_engagement", "").lower() in (
            "1", "true", "yes",
        )
        email_search = (args.get("q") or args.get("search") or "").strip() or None

        # One row per distinct email — no global ``COUNT(DISTINCT…)`` (too heavy on
        # multi‑million subscriber tables over remote MySQL).
        df, page_has_more = queries.get_users_by_email(
            date_from=date_from, date_to=date_to,
            list_id=list_id,     status=status,
            email_search=email_search,
            limit=limit,         offset=offset,
        )

        active_filters = {
            k: v for k, v in {
                "date_from": date_from,
                "date_to":   date_to,
                "list_id":   list_id,
                "status":    status,
                "q":         email_search,
            }.items() if v is not None
        }

        records = _clean_records(df)
        for r in records:
            if r.get("id") is not None:
                try:
                    r["id"] = int(r["id"])
                except (TypeError, ValueError):
                    pass
            subs = r.get("subscriber_ids")
            if isinstance(subs, list):
                try:
                    r["subscriber_ids"] = [int(x) for x in subs]
                except (TypeError, ValueError):
                    pass

        if include_engagement and records:
            ids_flat: list[int] = []
            seen_ids: set[int] = set()
            for r in records:
                subs = r.get("subscriber_ids")
                if isinstance(subs, list):
                    for x in subs:
                        try:
                            n = int(x)
                            if n not in seen_ids:
                                seen_ids.add(n)
                                ids_flat.append(n)
                        except (TypeError, ValueError):
                            continue
                elif r.get("id") is not None:
                    try:
                        n = int(r["id"])
                        if n not in seen_ids:
                            seen_ids.add(n)
                            ids_flat.append(n)
                    except (TypeError, ValueError):
                        pass
            if ids_flat:
                eng_raw, eng_meta = queries.build_engagement_for_subscribers(
                    ids_flat, date_from=date_from, date_to=date_to,
                )
                eng = queries.aggregate_engagement_by_email_rows(records, eng_raw)
            else:
                eng, eng_meta = {}, {
                    "total_campaigns": 0,
                    "uses_lists_column": True,
                    "scope_by_list": False,
                    "checked_campaigns": 0,
                    "queue_counts_used": False,
                }
            active_filters["include_engagement"] = True
            out = jsonify(
                {
                    "data":              records,
                    "pagination":        _pagination_meta(
                        None, limit, offset, has_more=page_has_more,
                    ),
                    "filters":           active_filters,
                    "engagement":        eng,
                    "engagement_meta":   eng_meta,
                }
            )
            out.headers["Cache-Control"] = "private, no-store"
            return out, 200

        return _etag_response(
            {
                "data":       records,
                "pagination": _pagination_meta(
                    None, limit, offset, has_more=page_has_more,
                ),
                "filters":    active_filters,
            },
            max_age=30,
        )

    # ------------------------------------------------------------------
    # /api/users/aggregate  – heavy counts (distinct emails + subscriber rows)
    # ------------------------------------------------------------------
    @app.get("/api/users/aggregate")
    @_handle_db_errors
    def users_aggregate():
        """
        Expensive aggregates for the Users summary card (same filters as ``/api/users``).

        Query parameters match ``/api/users`` (date_from, date_to, list_id, status, q/search).
        """
        args = request.args
        date_from, date_to = _collect_date_filters(args)
        list_id = _parse_int(args["list_id"], "list_id") if "list_id" in args else None
        status  = args.get("status") or None
        email_search = (args.get("q") or args.get("search") or "").strip() or None

        distinct = queries.count_users_distinct_email(
            date_from=date_from, date_to=date_to,
            list_id=list_id, status=status, email_search=email_search,
        )
        rows = queries.count_users(
            date_from=date_from, date_to=date_to,
            list_id=list_id, status=status, email_search=email_search,
        )
        active_filters = {
            k: v for k, v in {
                "date_from": date_from,
                "date_to":   date_to,
                "list_id":   list_id,
                "status":    status,
                "q":         email_search,
            }.items() if v is not None
        }
        out = jsonify(
            {
                "distinct_emails":  distinct,
                "subscriber_rows":  rows,
                "filters":          active_filters,
            }
        )
        out.headers["Cache-Control"] = "private, no-store"
        return out, 200

    # ------------------------------------------------------------------
    # /api/campaigns/<id>/metrics  – Sendy-style stats for one campaign (expand list)
    # ------------------------------------------------------------------
    @app.get("/api/campaigns/<int:campaign_id>/metrics")
    @_handle_db_errors
    def campaign_metrics(campaign_id: int):
        """Opens, clicks, bounces, unsubs, rates — same rules as list ``get_campaigns_stats``."""
        df = queries.get_campaign_metrics_by_id(campaign_id)
        if df is None or len(df) == 0:
            return (
                jsonify(
                    {
                        "error": "Campaign not found",
                        "campaign_id": campaign_id,
                    }
                ),
                404,
            )
        out = jsonify(_clean_records(df)[0])
        out.headers["Cache-Control"] = "private, no-store, max-age=0, must-revalidate"
        out.headers["Pragma"] = "no-cache"
        return out, 200

    # ------------------------------------------------------------------
    # /api/engagement  – per-subscriber opens/clicks for a list of IDs
    # ------------------------------------------------------------------
    @app.route("/api/engagement", methods=["GET", "POST"])
    @_handle_db_errors
    def engagement():
        """
        Return email-engagement stats for a set of subscriber IDs.

        **GET** — ``?ids=1,2,3&date_from=&date_to=`` (IDs comma-separated).

        **POST** — JSON ``{"ids": [1,2,3], "date_from": "...", "date_to": "..."}``
        (preferred: no URL length limit, no browser ETag/304 empty-body issues).

        Does **not** use ETag/304 — the dashboard must always receive JSON.
        """
        if request.method == "POST" and request.is_json:
            body = request.get_json(silent=True) or {}
            raw = body.get("ids")
            if isinstance(raw, list):
                sub_ids = []
                for x in raw:
                    try:
                        n = int(x)
                        if n > 0:
                            sub_ids.append(n)
                    except (TypeError, ValueError):
                        continue
            elif isinstance(raw, str):
                sub_ids = [int(x) for x in raw.split(",") if x.strip().isdigit()]
            else:
                sub_ids = []

            df_raw = (body.get("date_from") or "").strip()
            dt_raw = (body.get("date_to") or "").strip()
            date_from = _parse_date(df_raw, "date_from") if df_raw else None
            date_to   = _parse_date(dt_raw, "date_to")   if dt_raw else None
        else:
            args = request.args
            date_from, date_to = _collect_date_filters(args)
            ids_param = args.get("ids", "")
            sub_ids = [int(x) for x in ids_param.split(",") if x.strip().isdigit()]

        if not sub_ids:
            out = jsonify(
                {
                    "data":               {},
                    "total_campaigns":    0,
                    "uses_lists_column":  True,
                    "scope_by_list":      False,
                    "checked_campaigns":    0,
                    "queue_counts_used":  False,
                }
            )
            out.headers["Cache-Control"] = "private, no-store"
            return out, 200

        result, meta = queries.build_engagement_for_subscribers(
            sub_ids, date_from=date_from, date_to=date_to,
        )
        out = jsonify(
            {
                "data":              result,
                "total_campaigns":   meta["total_campaigns"],
                "uses_lists_column": meta["uses_lists_column"],
                "scope_by_list":     meta["scope_by_list"],
                "checked_campaigns": meta["checked_campaigns"],
                "queue_counts_used": meta.get("queue_counts_used", False),
            }
        )
        out.headers["Cache-Control"] = "private, no-store"
        return out, 200

    # ------------------------------------------------------------------
    # /api/cache   – cache admin (info + manual invalidation)
    # ------------------------------------------------------------------
    @app.get("/api/cache")
    def get_cache_info():
        """Return the current cache backend state (key count, memory, etc.)."""
        info = cache.cache_info()
        info["redis_url_configured"] = bool(os.getenv("REDIS_URL", "").strip())
        return jsonify({"cache": info}), 200

    @app.post("/api/cache/clear")
    def clear_cache():
        """Flush all cached entries.  Use after bulk data imports."""
        result = cache.cache_clear()
        logger.info("Cache cleared manually: %s", result)
        return jsonify({"status": "cleared", **result}), 200

    # ------------------------------------------------------------------
    # /api/stats
    # ------------------------------------------------------------------
    @app.get("/api/stats")
    @_handle_db_errors
    def get_stats():
        """
        Return aggregated statistics with GROUP BY / COUNT.

        Query parameters
        ----------------
        type      : str  (default: "subscribers")
            ``subscribers`` – growth over time, grouped by period + list
            ``campaigns``   – open / click / bounce rates per campaign
        date_from : YYYY-MM-DD  – lower bound (created_at or send_date)
        date_to   : YYYY-MM-DD  – upper bound
        list_id   : int         – (subscribers only) restrict to one list
        group_by  : day|month   – (subscribers only) time granularity
        status    : str         – (campaigns only) filter by campaign status

        Response shape
        --------------
        200 {
              "type": "<subscribers|campaigns>",
              "count": N,
              "filters": { <only active filters> },
              "data": [ { <no null fields> }, ... ]
            }
        """
        args     = request.args
        stat_type = args.get("type", "subscribers").lower().strip()

        date_from, date_to = _collect_date_filters(args)
        list_id  = _parse_int(args["list_id"], "list_id") if "list_id" in args else None
        status   = args.get("status") or None

        active_filters = {
            k: v for k, v in {
                "date_from": date_from,
                "date_to":   date_to,
                "list_id":   list_id,
                "status":    status,
            }.items() if v is not None
        }

        if stat_type == "subscribers":
            group_by = args.get("group_by", "day").lower().strip()
            df = queries.get_subscriber_stats(
                date_from=date_from,
                date_to=date_to,
                list_id=list_id,
                group_by=group_by,   # range-validated inside queries layer
            )
            return _etag_response(
                {
                    "type":     stat_type,
                    "group_by": group_by,
                    "count":    len(df),
                    "filters":  active_filters,
                    "data":     _clean_records(df),
                },
                max_age=300,   # matches @cached(ttl=300) in queries
            )

        if stat_type == "campaigns":
            camp_limit = None
            raw_cl = args.get("campaign_limit")
            if raw_cl is not None and str(raw_cl).strip() != "":
                camp_limit = _parse_int(str(raw_cl).strip(), "campaign_limit")
                camp_limit = min(camp_limit, queries.CAMPAIGN_PERF_MAX)

            # Fast path: campaigns table only (no ``opens``/``links`` longtext).  Full
            # ``get_campaign_performance`` times out remote MySQL on large ranges.
            df = queries.get_campaigns_sent_in_range_list(
                date_from=date_from,
                date_to=date_to,
                limit=camp_limit,
            )
            totals  = queries.get_email_totals(date_from=date_from, date_to=date_to)
            tot_row = totals.iloc[0] if len(totals) else None
            total_c = int(tot_row["campaigns_sent"]) if tot_row is not None else len(df)
            if camp_limit is not None:
                active_filters = {**active_filters, "campaign_limit": camp_limit}

            engagement_summary = None
            try:
                # OPTIMIZED: avoid duplicate get_email_totals call per request
                raw_es = queries.get_campaign_engagement_summary(
                    date_from=date_from,
                    date_to=date_to,
                    status=status,
                    email_totals=totals,
                )
                cir = int(raw_es.get("campaigns_in_range") or 0)
                if cir > 0:
                    engagement_summary = {
                        "open_rate_pct":      raw_es.get("open_rate_pct"),
                        "click_rate_pct":     raw_es.get("click_rate_pct"),
                        "sample_campaigns":   raw_es.get("sample_campaigns"),
                        "campaigns_in_range": raw_es.get("campaigns_in_range"),
                        "campaigns_included": raw_es.get("campaigns_included"),
                        "full_coverage":      raw_es.get("full_coverage"),
                        "sample_mode":        raw_es.get("sample_mode"),
                        "sample_offset_rows": raw_es.get("sample_offset_rows"),
                        "query_date_from":    raw_es.get("query_date_from"),
                        "query_date_to":      raw_es.get("query_date_to"),
                    }
            except (OperationalError, SQLAlchemyError) as exc:
                current_app.logger.warning(
                    "campaign engagement summary skipped: %s", exc,
                )

            # No ETag / browser cache — date filters must always refetch fresh
            # engagement + totals (see ``get_campaign_performance`` uncached in queries).
            out = jsonify(
                {
                    "type":              stat_type,
                    "count":             len(df),
                    "total_emails_sent": int(tot_row["total_emails_sent"]) if tot_row is not None else 0,
                    "campaigns_sent":    total_c,
                    "campaigns_returned": len(df),
                    "campaigns_truncated": len(df) < total_c,
                    "campaign_list_mode": "light",
                    "engagement_summary": engagement_summary,
                    "filters":           active_filters,
                    "data":              _clean_records(df),
                }
            )
            out.headers["Cache-Control"] = "private, no-store, max-age=0, must-revalidate"
            out.headers["Pragma"] = "no-cache"
            return out, 200

        valid = "subscribers, campaigns"
        raise ValueError(f"Unknown stat type '{stat_type}'. Valid: {valid}")


def _dispatch(resource: str, args):
    """
    Map the *resource* query parameter to the correct query function.

    Raises ValueError for unknown resources or invalid parameters.
    """
    match resource:
        case "subscribers":
            raw_list = args.get("list_id")
            if raw_list is not None:
                list_id = _parse_int(raw_list, "list_id")
                return queries.get_subscribers_by_list(list_id)
            return queries.get_all_subscribers()

        case "campaigns":
            raw_campaign = args.get("campaign_id")
            if raw_campaign is not None:
                campaign_id = _parse_int(raw_campaign, "campaign_id")
                return queries.get_campaign_by_id(campaign_id)
            return queries.get_all_campaigns()

        case "campaign_stats":
            return queries.get_campaign_stats()

        case "active_counts":
            return queries.get_active_subscriber_counts()

        case _:
            valid = "subscribers, campaigns, campaign_stats, active_counts"
            raise ValueError(
                f"Unknown resource '{resource}'. Valid options: {valid}"
            )


# ---------------------------------------------------------------------------
# Error handlers
# ---------------------------------------------------------------------------

def _register_error_handlers(app: Flask) -> None:

    @app.errorhandler(404)
    def not_found(exc):
        return jsonify({"error": "Endpoint not found"}), 404

    @app.errorhandler(405)
    def method_not_allowed(exc):
        return jsonify({"error": "Method not allowed"}), 405

    @app.errorhandler(500)
    def internal_error(exc):
        logger.exception("Unhandled exception")
        return jsonify({"error": "Internal server error"}), 500


# ---------------------------------------------------------------------------
# Dev entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    host = os.getenv("FLASK_HOST", "127.0.0.1")
    port = int(os.getenv("FLASK_PORT", 5000))
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"

    app = create_app()
    logger.info(
        "Dev server: FLASK_DEBUG=%s (Python auto-reload %s). "
        "TEMPLATES_AUTO_RELOAD=%s — see .env.example.",
        debug,
        "ON" if debug else "OFF — restart process after editing .py files",
        app.config.get("TEMPLATES_AUTO_RELOAD"),
    )
    # use_reloader watches .py files and restarts the process when debug=True
    app.run(host=host, port=port, debug=debug, use_reloader=debug)
