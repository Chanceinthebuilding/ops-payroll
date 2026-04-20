"""
월별 상품화 개수·FM 인건비(물류 포함/미포함) 집계.

- 상품화 개수: inspection.items (sellable, registration_type=personal, provider_requested_at)
- 인건비: COMMERCIALIZATION_LABOR_MONTHLY_SQL 로 월별 fm_krw, logistics_krw 조회 (선택)
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))


def _parse_ym(s: str) -> tuple[int, int]:
    s = (s or "").strip()
    parts = s.replace("/", "-").split("-")
    if len(parts) < 2:
        raise ValueError(f"invalid ym: {s!r}")
    return int(parts[0]), int(parts[1])


def _first_of_month_kst(y: int, m: int) -> datetime:
    return datetime(y, m, 1, 0, 0, 0, tzinfo=KST)


def _month_end_exclusive_kst(y: int, m: int) -> datetime:
    """Return first instant of the month after (y,m) in KST."""
    if m == 12:
        return datetime(y + 1, 1, 1, 0, 0, 0, tzinfo=KST)
    return datetime(y, m + 1, 1, 0, 0, 0, tzinfo=KST)


def default_range_ym() -> tuple[str, str]:
    """(start_ym, end_ym inclusive) 기본: 2023-10 ~ 이번 달."""
    start = os.environ.get("COMMERCIALIZATION_RANGE_START", "2023-10").strip() or "2023-10"
    now_kst = datetime.now(KST)
    end = f"{now_kst.year}-{now_kst.month:02d}"
    return start, end


def _first_env(*keys: str) -> str:
    for k in keys:
        v = (os.environ.get(k) or "").strip()
        if v:
            return v
    return ""


def _db_config() -> dict[str, str]:
    """
    DB 접속 설정을 환경변수에서 읽는다.
    지원 키:
    - DATABASE_URL
    - DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD
    - host/port/database/user/password
    """
    dsn = _first_env("DATABASE_URL", "database_url")
    host = _first_env("DB_HOST", "host")
    port = _first_env("DB_PORT", "port") or "5432"
    name = _first_env("DB_NAME", "database")
    user = _first_env("DB_USER", "user")
    pw = _first_env("DB_PASSWORD", "password")
    sslmode = _first_env("DB_SSLMODE", "PGSSLMODE")
    return {
        "dsn": dsn,
        "host": host,
        "port": port,
        "name": name,
        "user": user,
        "password": pw,
        "sslmode": sslmode,
    }


def has_db_config() -> bool:
    cfg = _db_config()
    if cfg["dsn"]:
        return True
    return bool(cfg["host"] and cfg["name"] and cfg["user"] and cfg["password"])


def db_config_error_message() -> str:
    return (
        "DB 연결 정보가 없습니다. "
        "환경변수 DATABASE_URL 또는 "
        "(DB_HOST/DB_NAME/DB_USER/DB_PASSWORD) "
        "또는 (host/database/user/password) 를 설정해 주세요."
    )


def _db_connect():
    import psycopg2

    cfg = _db_config()
    timeout = int(os.environ.get("DB_CONNECT_TIMEOUT", "15"))
    if cfg["dsn"]:
        return psycopg2.connect(cfg["dsn"], connect_timeout=timeout)
    if not (cfg["host"] and cfg["name"] and cfg["user"] and cfg["password"]):
        raise RuntimeError(db_config_error_message())
    kwargs: dict[str, Any] = {
        "host": cfg["host"],
        "port": cfg["port"],
        "dbname": cfg["name"],
        "user": cfg["user"],
        "password": cfg["password"],
        "connect_timeout": timeout,
    }
    if cfg["sslmode"]:
        kwargs["sslmode"] = cfg["sslmode"]
    return psycopg2.connect(
        **kwargs,
    )


def _items_extra_where() -> str:
    raw = (os.environ.get("COMMERCIALIZATION_ITEMS_EXTRA_WHERE") or "").strip()
    if not raw:
        return ""
    if not raw.upper().startswith("AND"):
        return " AND " + raw
    return " " + raw


def _productized_sql() -> str:
    extra = _items_extra_where()
    return f"""
SELECT
  to_char(
    date_trunc('month', (i.provider_requested_at AT TIME ZONE 'Asia/Seoul')),
    'YYYY-MM'
  ) AS ym,
  COUNT(*)::bigint AS cnt
FROM inspection.items i
WHERE i.sellable IS TRUE
  AND i.provider_requested_at IS NOT NULL
  AND i.registration_type = 'personal'
  {extra}
  AND i.provider_requested_at >= %s::timestamptz
  AND i.provider_requested_at < %s::timestamptz
GROUP BY 1
ORDER BY 1
"""


def _execute_with_retry(cur, sql: str, params: tuple[Any, ...], retries: int = 3) -> None:
    from psycopg2.errors import SerializationFailure

    delay = 0.8
    for attempt in range(retries):
        try:
            cur.execute(sql, params)
            return
        except SerializationFailure:
            if attempt == retries - 1:
                raise
            time.sleep(delay)
            delay *= 1.8


def fetch_productized_monthly(start_ym: str, end_ym_inclusive: str) -> tuple[dict[str, int], str | None]:
    """
    Returns (ym -> count, error_message_or_none).
    """
    try:
        sy, sm = _parse_ym(start_ym)
        ey, em = _parse_ym(end_ym_inclusive)
    except ValueError as e:
        return {}, str(e)

    start_dt = _first_of_month_kst(sy, sm)
    end_ex = _month_end_exclusive_kst(ey, em)

    sql = _productized_sql()
    try:
        conn = _db_connect()
    except Exception as e:
        logger.exception("commercialization DB connect")
        return {}, f"DB 연결 실패: {e}"

    out: dict[str, int] = {}
    try:
        with conn:
            with conn.cursor() as cur:
                _execute_with_retry(cur, sql, (start_dt, end_ex))
                for row in cur.fetchall():
                    ym = str(row[0])
                    out[ym] = int(row[1])
    except Exception as e:
        logger.exception("commercialization productized query")
        return {}, f"상품화 집계 쿼리 실패: {e}"
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return out, None


def fetch_labor_monthly() -> tuple[dict[str, dict[str, float]], str | None]:
    """
    COMMERCIALIZATION_LABOR_MONTHLY_SQL 이 있으면 실행.
    결과 컬럼: ym (text 또는 date), fm_krw, logistics_krw

    Returns ({ym: {"fm": x, "logistics": y}}, error_or_none)
    """
    sql = (os.environ.get("COMMERCIALIZATION_LABOR_MONTHLY_SQL") or "").strip()
    if not sql:
        return {}, None

    try:
        conn = _db_connect()
    except Exception as e:
        return {}, f"DB 연결 실패(인건비): {e}"

    out: dict[str, dict[str, float]] = {}
    try:
        with conn:
            with conn.cursor() as cur:
                _execute_with_retry(cur, sql, ())
                cols = [d[0] for d in (cur.description or [])]
                lower = [c.lower() for c in cols]
                if len(lower) < 3:
                    return {}, "COMMERCIALIZATION_LABOR_MONTHLY_SQL 결과에 ym, fm_krw, logistics_krw 가 필요합니다."

                def _idx(name: str) -> int:
                    for i, c in enumerate(lower):
                        if c == name:
                            return i
                    return -1

                i_ym = _idx("ym")
                i_fm = _idx("fm_krw")
                i_log = _idx("logistics_krw")
                if i_ym < 0 or i_fm < 0 or i_log < 0:
                    return {}, "컬럼명은 ym, fm_krw, logistics_krw 를 사용해 주세요."

                for row in cur.fetchall():
                    raw_ym = row[i_ym]
                    if hasattr(raw_ym, "strftime"):
                        ym = raw_ym.strftime("%Y-%m")
                    else:
                        s = str(raw_ym).strip()
                        ym = s[:7] if len(s) >= 7 else s
                    out[ym] = {
                        "fm": float(row[i_fm] or 0),
                        "logistics": float(row[i_log] or 0),
                    }
    except Exception as e:
        logger.exception("commercialization labor query")
        return {}, f"인건비 집계 쿼리 실패: {e}"
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return out, None


def load_remarks(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
    except Exception:
        pass
    return {}


def _pct_change(curr: float | None, prev: float | None) -> float | None:
    if curr is None or prev is None:
        return None
    if prev == 0:
        return None
    return round((curr - prev) / prev * 100.0, 1)


def _sorted_months(keys: set[str]) -> list[str]:
    return sorted(keys)


def build_table_rows(
    productized: dict[str, int],
    labor: dict[str, dict[str, float]],
    remarks: dict[str, str],
    *,
    include_logistics: bool,
) -> list[dict[str, Any]]:
    """MoM 변화율 포함 행 목록."""
    keys = set(productized.keys()) | set(labor.keys())
    months = _sorted_months(keys)
    rows: list[dict[str, Any]] = []

    prev_cnt: int | None = None
    prev_cost: float | None = None
    prev_unit: float | None = None

    for ym in months:
        cnt = int(productized.get(ym, 0))
        lab = labor.get(ym) or {}
        fm = float(lab.get("fm", 0.0))
        logistics = float(lab.get("logistics", 0.0))
        total_cost = fm + (logistics if include_logistics else 0.0)

        unit = (total_cost / cnt) if cnt > 0 else None

        row = {
            "ym": ym,
            "cnt": cnt,
            "chg_cnt": _pct_change(float(cnt), float(prev_cnt)) if prev_cnt is not None else None,
            "cost": int(round(total_cost)),
            "chg_cost": _pct_change(total_cost, prev_cost) if prev_cost is not None else None,
            "unit": int(round(unit)) if unit is not None else None,
            "chg_unit": _pct_change(unit, prev_unit) if prev_unit is not None and unit is not None else None,
            "remark": remarks.get(ym, ""),
            "has_labor": bool(lab),
        }
        rows.append(row)

        prev_cnt = cnt
        prev_cost = total_cost
        prev_unit = unit

    return rows


def fmt_pct(v: float | None) -> str:
    if v is None:
        return "—"
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.1f}%"
