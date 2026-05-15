from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
YM_RE = re.compile(r"^\d{4}-\d{2}$")


def _parse_int(v: str) -> int:
    s = (v or "").strip().replace(",", "")
    if not s or s == "-":
        return 0
    return int(float(s))


def _parse_pct(v: str) -> float | None:
    s = (v or "").strip()
    if not s or s == "-":
        return None
    if s.endswith("%"):
        s = s[:-1]
    try:
        return float(s)
    except ValueError:
        return None


def fmt_pct(v: float | None) -> str:
    if v is None:
        return "—"
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.1f}%"


def _credentials_info() -> dict[str, Any]:
    raw = (os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON") or "").strip()
    if raw:
        info = json.loads(raw)
        if not isinstance(info, dict):
            raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS_JSON은 JSON 객체여야 합니다.")
        return info

    file_path = (os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    candidates = [
        Path(file_path) if file_path else None,
        ROOT / ".keys" / "ops-robot-keys.json",
        ROOT / "credentials" / "service_account.json",
    ]
    for p in candidates:
        if p and p.is_file():
            return json.loads(p.read_text(encoding="utf-8"))

    raise RuntimeError(
        "구글 인증 정보가 없습니다. GOOGLE_APPLICATION_CREDENTIALS_JSON 또는 서비스 계정 키 파일을 설정해 주세요."
    )


def _sheet_config() -> tuple[str, str]:
    sheet_id = (os.environ.get("COMMERCIALIZATION_SHEET_ID") or "").strip()
    tab_name = (os.environ.get("COMMERCIALIZATION_SHEET_TAB") or "").strip() or "상품화 데이터"
    if not sheet_id:
        sheet_id = "1_NuP4yFBdePnBttUNv_Ag5MmXYOpWovv3UN53vMJmQ0"
    return sheet_id, tab_name


def _read_sheet_values() -> list[list[str]]:
    import gspread
    from google.oauth2.service_account import Credentials

    info = _credentials_info()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)

    sheet_id, tab_name = _sheet_config()
    ws = gc.open_by_key(sheet_id).worksheet(tab_name)
    return ws.get_all_values()


def _parse_block(values: list[list[str]], start_col: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in values[3:]:
        if len(row) <= start_col:
            continue
        ym = (row[start_col] or "").strip()
        if not YM_RE.match(ym):
            continue

        def _cell(idx: int) -> str:
            return row[idx] if idx < len(row) else ""

        rows.append(
            {
                "ym": ym,
                "cnt": _parse_int(_cell(start_col + 1)),
                "chg_cnt": _parse_pct(_cell(start_col + 2)),
                "cost": _parse_int(_cell(start_col + 3)),
                "chg_cost": _parse_pct(_cell(start_col + 4)),
                "unit": _parse_int(_cell(start_col + 5)),
                "chg_unit": _parse_pct(_cell(start_col + 6)),
                "remark": (_cell(start_col + 7) or "").strip(),
            }
        )
    return rows


def _filter_range(rows: list[dict[str, Any]], start: str, end: str) -> list[dict[str, Any]]:
    if not start or not end:
        return rows
    return [r for r in rows if start <= r["ym"] <= end]


def default_range_ym() -> tuple[str, str]:
    values = _read_sheet_values()
    base_rows = _parse_block(values, 0)
    if not base_rows:
        return "", ""
    months = sorted({str(r["ym"]) for r in base_rows})
    return months[0], months[-1]


def fetch_dashboard_rows(start: str, end: str) -> tuple[dict[str, Any], str | None]:
    try:
        values = _read_sheet_values()
    except Exception as e:
        return {}, f"구글 시트 조회 실패: {e}"

    rows_fm = _filter_range(_parse_block(values, 0), start, end)
    rows_log = _filter_range(_parse_block(values, 9), start, end)
    rows_order = _filter_range(_parse_block(values, 18), start, end)
    return {
        "rows_fm": rows_fm,
        "rows_logistics": rows_log,
        "rows_order_fm": rows_order,
    }, None
