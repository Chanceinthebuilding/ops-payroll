"""
Flask 웹 앱: 시프티 엑셀 업로드 → 실시간 급여·근무 결과 확인
실행: flask --app app run (또는 python app.py)
"""
import json
import logging
import os
import shutil
import sys
import tempfile
import time
import yaml
from datetime import date, datetime, timedelta, timezone
from functools import wraps
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from flask import Flask, make_response, redirect, render_template, request, flash, session, jsonify, url_for
from werkzeug.middleware.proxy_fix import ProxyFix

from auth_google import auth_disabled, register_google_auth

ROOT = Path(__file__).resolve().parent
if load_dotenv is not None:
    # 실행 위치(cwd)와 무관하게 프로젝트 루트 .env를 우선 로드한다.
    load_dotenv(ROOT / ".env")
OUTPUT_BASE = ROOT / "output"
PUBLISHED_ID = "published"
PUBLISHED_DIR = OUTPUT_BASE / PUBLISHED_ID
PUBLISHED_FILES = (
    "daily_summary.csv",
    "payroll_result.csv",
    "anomaly_report.csv",
    "weekly_allowance_result.csv",
)
FM_ROSTER_FILENAME = "fm_roster.xlsx"
FM_ROSTER_LOCAL_DIR = OUTPUT_BASE / "metadata"
FM_ROSTER_LOCAL_PATH = FM_ROSTER_LOCAL_DIR / FM_ROSTER_FILENAME
FM_UPLOAD_META_LOCAL_PATH = FM_ROSTER_LOCAL_DIR / "fm_upload_meta.json"
COMMERCIALIZATION_REMARKS_PATH = FM_ROSTER_LOCAL_DIR / "commercialization_remarks.json"
LAST_LEAVE_LOCAL_PATH = FM_ROSTER_LOCAL_DIR / "last_leave.xlsx"
OVERTIME_STATUS_FILENAME = "overtime_status.csv"
OVERTIME_STATUS_META_FILENAME = "overtime_status_meta.json"
OVERTIME_STATUS_LOCAL_PATH = FM_ROSTER_LOCAL_DIR / OVERTIME_STATUS_FILENAME
OVERTIME_STATUS_META_LOCAL_PATH = FM_ROSTER_LOCAL_DIR / OVERTIME_STATUS_META_FILENAME
DASHBOARD_CACHE_FILENAME = "dashboard_cache.json"
DASHBOARD_CACHE_LOCAL_PATH = FM_ROSTER_LOCAL_DIR / DASHBOARD_CACHE_FILENAME
PERMISSIONS_FILENAME = "user_permissions.json"
PERMISSIONS_LOCAL_DIR = FM_ROSTER_LOCAL_DIR / "access_control"
PERMISSIONS_LOCAL_PATH = PERMISSIONS_LOCAL_DIR / PERMISSIONS_FILENAME
PERMISSION_SCOPE_KEYS = ("payroll", "dashboard", "overtime", "commercialization", "admin_data")
PERMISSION_LEVELS = ("none", "view", "edit")
PERMISSION_LEVEL_RANK = {"none": 0, "view": 1, "edit": 2}
_PERMISSIONS_CACHE: dict | None = None
_PERMISSIONS_CACHE_AT = 0.0
_PERMISSIONS_CACHE_TTL_SEC = int((os.environ.get("PERMISSIONS_CACHE_TTL_SEC") or "15").strip() or "15")
PUBLISHED_META_FILENAME = "meta.json"
KST = timezone(timedelta(hours=9))
sys.path.insert(0, str(ROOT))
CONTRACT_CONFIG_PATH = ROOT / "contract_config.yaml"

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "chaftee-payroll-secret")
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50MB
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=14)
# Railway(리버스 프록시) 뒤에서 원 요청의 scheme/host를 신뢰해 OAuth redirect_uri가 https로 생성되도록 보정
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
app.config["PREFERRED_URL_SCHEME"] = "https"
if os.environ.get("SESSION_COOKIE_SECURE", "").strip().lower() in ("1", "true", "yes"):
    app.config["SESSION_COOKIE_SECURE"] = True

register_google_auth(app)
_gcs_client = None
_gcs_storage_loaded = False
_gcs_storage_mod = None
logger = logging.getLogger(__name__)
_VIEW_CACHE: dict[str, dict] = {}
_VIEW_CACHE_TTL_SEC = int((os.environ.get("VIEW_CACHE_TTL_SEC") or "30").strip() or "30")


def _normalize_google_credentials_env() -> None:
    """
    GOOGLE_APPLICATION_CREDENTIALS_JSON(서비스 계정 JSON 문자열)이 있으면
    GOOGLE_APPLICATION_CREDENTIALS(파일 경로)는 무시한다. 둘 다 두면 google-auth 등이
    파일 경로를 읽으려다 Railway에서 PermissionError가 나는 경우가 많다.
    """
    if (os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON") or "").strip():
        removed = os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
        if removed:
            logger.info(
                "GOOGLE_APPLICATION_CREDENTIALS_JSON이 있어 GOOGLE_APPLICATION_CREDENTIALS(파일 경로)는 사용하지 않습니다."
            )


_normalize_google_credentials_env()

# 배포·업데이트마다 패치 번호를 올리거나, Railway 변수 APP_VERSION(예: 1.0.1 또는 v1.0.1)으로 덮어씀
APP_VERSION_DEFAULT = "1.0.0"


def app_version_display() -> str:
    raw = (os.environ.get("APP_VERSION") or "").strip()
    if not raw:
        raw = APP_VERSION_DEFAULT
    if raw.lower().startswith("v"):
        return raw
    return f"v{raw}"


@app.route("/login")
def login_page():
    if auth_disabled():
        return redirect(url_for("index"))
    if session.get("user_email"):
        return redirect(url_for("index"))
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    if auth_disabled():
        return redirect(url_for("index"))
    return redirect(url_for("login_page"))


@app.route("/dashboard")
def dashboard():
    if not _can_current_user("dashboard", "view"):
        flash("인건비 대시보드 조회 권한이 없습니다.", "error")
        return redirect(url_for("index"))
    if not _published_exists():
        return render_template("dashboard.html", dashboard_ready=False)
    pmeta = _read_published_meta_dict() or {}
    fm_meta = _read_fm_upload_meta_dict() or {}
    token = f"{pmeta.get('published_at','')}|{fm_meta.get('uploaded_at','')}"
    hit = _view_cache_get("dashboard", token)
    if hit is not None:
        return render_template("dashboard.html", **hit)

    pre = _read_dashboard_cache_dict() or {}
    pre_ctx = pre.get("ctx") if isinstance(pre, dict) else None
    if isinstance(pre_ctx, dict) and pre_ctx:
        _view_cache_set("dashboard", token, pre_ctx)
        return render_template("dashboard.html", **pre_ctx)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        if not _download_published_to_dir(tmp_dir):
            return render_template("dashboard.html", dashboard_ready=False)
        _attach_fm_roster_to_dir(tmp_dir)
        ctx = _build_dashboard_context(tmp_dir)
        _view_cache_set("dashboard", token, ctx)
        return render_template("dashboard.html", **ctx)


@app.route("/commercialization")
def commercialization_dashboard():
    """월별 상품화 FM 인건비 대시보드(구글 시트 연동)."""
    if not _can_current_user("commercialization", "view"):
        flash("상품화 인건비 조회 권한이 없습니다.", "error")
        return redirect(url_for("index"))
    from commercialization_sheet import (
        default_range_ym,
        fetch_dashboard_rows,
        fmt_pct,
    )

    start = (request.args.get("start") or "").strip()
    end = (request.args.get("end") or "").strip()
    if not start or not end:
        try:
            start, end = default_range_ym()
        except Exception as e:
            return render_template(
                "commercialization.html",
                commercialization_ready=False,
                commercialization_error=f"구글 시트 조회 실패: {e}",
                range_start="",
                range_end="",
                rows_fm=[],
                rows_logistics=[],
                rows_order_fm=[],
                fmt_pct=fmt_pct,
            )

    data, err = fetch_dashboard_rows(start, end)
    if err:
        return render_template(
            "commercialization.html",
            commercialization_ready=False,
            commercialization_error=err,
            range_start=start,
            range_end=end,
            rows_fm=[],
            rows_logistics=[],
            rows_order_fm=[],
            fmt_pct=fmt_pct,
        )

    rows_fm = list(data.get("rows_fm", []))
    rows_log = list(data.get("rows_logistics", []))
    rows_order = list(data.get("rows_order_fm", []))
    role_totals = _commercialization_role_totals_from_dashboard_cache()
    _apply_commercialization_role_override(rows_fm, rows_log, rows_order, role_totals, target_ym="2026-04")
    _apply_unit_color_scale(rows_fm)
    _apply_unit_color_scale(rows_log)
    _apply_unit_color_scale(rows_order)
    chart_ctx = _build_commercialization_unit_line_chart(rows_fm, rows_log, rows_order, y_min=0, y_max=6000)

    return render_template(
        "commercialization.html",
        commercialization_ready=True,
        commercialization_error=err,
        range_start=start,
        range_end=end,
        rows_fm=rows_fm,
        rows_logistics=rows_log,
        rows_order_fm=rows_order,
        chart_ctx=chart_ctx,
        fmt_pct=fmt_pct,
    )


@app.route("/healthz")
def healthz():
    return jsonify({"ok": True, "version": app_version_display()}), 200


def _gcs_bucket_name() -> str:
    return os.environ.get("GCS_BUCKET", "").strip()


def _commercialization_role_totals_from_dashboard_cache() -> dict[str, int]:
    rows: list[dict] = []

    payload = _read_dashboard_cache_dict() or {}
    ctx = payload.get("ctx") if isinstance(payload, dict) else {}
    cached_rows = ctx.get("fm_role_rows") if isinstance(ctx, dict) else []
    if isinstance(cached_rows, list) and cached_rows:
        rows = cached_rows

    # 캐시가 비어 있으면 published 데이터를 읽어 즉시 집계(최신 업로드 직후 대비).
    if not rows and _published_exists():
        try:
            with tempfile.TemporaryDirectory() as tmp:
                tmp_dir = Path(tmp)
                if _download_published_to_dir(tmp_dir):
                    _attach_fm_roster_to_dir(tmp_dir)
                    live_ctx = _build_dashboard_context(tmp_dir)
                    live_rows = live_ctx.get("fm_role_rows") if isinstance(live_ctx, dict) else []
                    if isinstance(live_rows, list):
                        rows = live_rows
        except Exception:
            rows = []

    if not isinstance(rows, list):
        rows = []

    def _norm(s: str) -> str:
        return str(s or "").strip().replace(" ", "")

    tagging = 0
    cleaning = 0
    shooting = 0
    logistics = 0
    for r in rows:
        role = _norm(r.get("role"))
        try:
            pay = int(round(float(r.get("total_pay", 0) or 0)))
        except (TypeError, ValueError):
            pay = 0
        if "태깅" in role:
            tagging += pay
        if "클리닝" in role:
            cleaning += pay
        if "촬영" in role:
            shooting += pay
        if "물류" in role:
            logistics += pay

    return {
        "tagging_krw": tagging,
        "cleaning_krw": cleaning,
        "shooting_krw": shooting,
        "logistics_krw": logistics,
    }


def _recalculate_cost_change_fields(rows: list[dict]) -> None:
    prev_cost: int | None = None
    prev_unit: int | None = None
    for r in rows:
        cost = int(r.get("cost") or 0)
        unit = r.get("unit")
        unit_val = int(unit) if unit is not None else None

        if prev_cost is None or prev_cost == 0:
            r["chg_cost"] = None
        else:
            r["chg_cost"] = round((cost - prev_cost) / prev_cost * 100.0, 1)

        if prev_unit is None or prev_unit == 0 or unit_val is None:
            r["chg_unit"] = None
        else:
            r["chg_unit"] = round((unit_val - prev_unit) / prev_unit * 100.0, 1)

        prev_cost = cost
        prev_unit = unit_val


def _apply_unit_color_scale(rows: list[dict]) -> None:
    values = [int(r.get("unit")) for r in rows if r.get("unit") is not None]
    if not values:
        for r in rows:
            r["unit_bg"] = "#ffffff"
        return

    lo = min(values)
    hi = max(values)
    mid = (lo + hi) / 2.0

    def _lerp(a: int, b: int, t: float) -> int:
        return int(round(a + (b - a) * t))

    def _rgb_to_hex(rgb: tuple[int, int, int]) -> str:
        return "#{:02x}{:02x}{:02x}".format(*rgb)

    # 단가형 지표: 낮을수록 좋으므로 Green(낮음) → White(중간) → Red(높음)
    green = (74, 222, 128)
    white = (255, 255, 255)
    red = (248, 113, 113)

    for r in rows:
        v = r.get("unit")
        if v is None or lo == hi:
            r["unit_bg"] = "#ffffff"
            continue
        fv = float(v)
        if fv <= mid:
            denom = max(1e-9, (mid - lo))
            t = (fv - lo) / denom
            rgb = (
                _lerp(green[0], white[0], t),
                _lerp(green[1], white[1], t),
                _lerp(green[2], white[2], t),
            )
        else:
            denom = max(1e-9, (hi - mid))
            t = (fv - mid) / denom
            rgb = (
                _lerp(white[0], red[0], t),
                _lerp(white[1], red[1], t),
                _lerp(white[2], red[2], t),
            )
        r["unit_bg"] = _rgb_to_hex(rgb)


def _apply_commercialization_role_override(
    rows_fm: list[dict],
    rows_logistics: list[dict],
    rows_order_fm: list[dict],
    role_totals: dict[str, int],
    *,
    target_ym: str,
    tax_apply_from_ym: str = "2026-04",
    tax_multiplier: float = 0.9,
) -> None:
    base_fm = int(role_totals.get("tagging_krw", 0)) + int(role_totals.get("cleaning_krw", 0)) + int(
        role_totals.get("shooting_krw", 0)
    )
    logistics = int(role_totals.get("logistics_krw", 0))
    if base_fm <= 0 and logistics <= 0:
        return

    def _ensure_row(rows: list[dict]) -> dict:
        for rr in rows:
            if str(rr.get("ym")) == target_ym:
                return rr
        new_row = {
            "ym": target_ym,
            "cnt": 0,
            "chg_cnt": None,
            "cost": 0,
            "chg_cost": None,
            "unit": None,
            "chg_unit": None,
            "remark": "",
        }
        rows.append(new_row)
        rows.sort(key=lambda x: str(x.get("ym") or ""))
        return new_row

    row_fm = _ensure_row(rows_fm)
    # 2026-04부터는 세금 10% 차감(= 0.9배) 기준으로 기입
    if target_ym >= tax_apply_from_ym:
        base_fm = int(round(base_fm * tax_multiplier))
        logistics = int(round(logistics * tax_multiplier))

    row_fm["cost"] = base_fm
    cnt = int(row_fm.get("cnt") or 0)
    row_fm["unit"] = int(round(base_fm / cnt)) if cnt > 0 else None
    row_fm["remark"] = (str(row_fm.get("remark") or "") + " | 역할별 합계 반영").strip(" |")

    row_log = _ensure_row(rows_logistics)
    total = base_fm + logistics
    row_log["cost"] = total
    cnt = int(row_log.get("cnt") or 0)
    row_log["unit"] = int(round(total / cnt)) if cnt > 0 else None
    row_log["remark"] = (str(row_log.get("remark") or "") + " | 역할별 합계 반영").strip(" |")

    row_order = _ensure_row(rows_order_fm)
    row_order["cost"] = logistics
    cnt = int(row_order.get("cnt") or 0)
    row_order["unit"] = int(round(logistics / cnt)) if cnt > 0 else None
    row_order["remark"] = (str(row_order.get("remark") or "") + " | 역할별 합계 반영").strip(" |")

    _recalculate_cost_change_fields(rows_fm)
    _recalculate_cost_change_fields(rows_logistics)
    _recalculate_cost_change_fields(rows_order_fm)


def _build_commercialization_unit_line_chart(
    rows_fm: list[dict],
    rows_logistics: list[dict],
    rows_order_fm: list[dict],
    *,
    y_min: int,
    y_max: int,
) -> dict:
    labels = sorted({str(r.get("ym")) for r in (rows_fm + rows_logistics + rows_order_fm) if r.get("ym")})
    width = 1380
    height = 340
    pad_l, pad_r, pad_t, pad_b = 70, 26, 20, 62
    plot_w = max(1, width - pad_l - pad_r)
    plot_h = max(1, height - pad_t - pad_b)

    def _x(i: int, n: int) -> float:
        if n <= 1:
            return float(pad_l + plot_w / 2)
        return float(pad_l + (plot_w * i / (n - 1)))

    def _y(v: int | None) -> float | None:
        if v is None:
            return None
        vv = max(y_min, min(y_max, int(v)))
        ratio = (vv - y_min) / max(1, (y_max - y_min))
        return float(pad_t + (1.0 - ratio) * plot_h)

    def _series(rows: list[dict], key: str, color: str) -> dict:
        by = {str(r.get("ym")): r for r in rows}
        points_xy: list[str] = []
        dots: list[dict] = []
        values_by_month: dict[str, int | None] = {}
        for i, ym in enumerate(labels):
            rv = by.get(ym) or {}
            v = rv.get("unit")
            values_by_month[ym] = int(v) if v is not None else None
            xv = _x(i, len(labels))
            yv = _y(int(v) if v is not None else None)
            if yv is None:
                continue
            points_xy.append(f"{xv:.2f},{yv:.2f}")
            dots.append({"x": round(xv, 2), "y": round(yv, 2), "label": ym, "value": int(v)})
        return {"key": key, "color": color, "points": " ".join(points_xy), "dots": dots, "values_by_month": values_by_month}

    ticks = []
    for v in [0, 1500, 3000, 4500, 6000]:
        yv = _y(v)
        if yv is None:
            continue
        ticks.append({"value": v, "y": round(yv, 2)})

    step = max(1, (len(labels) + 11) // 12)
    x_ticks = [
        {"label": ym, "x": round(_x(i, len(labels)), 2), "show": (i % step == 0) or (i == len(labels) - 1)}
        for i, ym in enumerate(labels)
    ]

    month_tooltips: dict[str, dict] = {}
    month_bands: list[dict] = []
    n = len(labels)
    for i, ym in enumerate(labels):
        x_curr = _x(i, n)
        x_prev = _x(i - 1, n) if i > 0 else x_curr
        x_next = _x(i + 1, n) if i < (n - 1) else x_curr
        left = (x_prev + x_curr) / 2 if i > 0 else x_curr - ((x_next - x_curr) / 2 if n > 1 else 18)
        right = (x_curr + x_next) / 2 if i < (n - 1) else x_curr + ((x_curr - x_prev) / 2 if n > 1 else 18)
        month_bands.append({"ym": ym, "x": round(left, 2), "w": round(max(6.0, right - left), 2)})
        month_tooltips[ym] = {"ym": ym}

    series_rows = [
        _series(rows_fm, "상품화 FM", "#2563eb"),
        _series(rows_logistics, "상품화+물류 FM", "#16a34a"),
        _series(rows_order_fm, "주문 FM", "#7c3aed"),
    ]
    for s in series_rows:
        key = s["key"]
        for ym in labels:
            month_tooltips[ym][key] = s["values_by_month"].get(ym)

    return {
        "width": width,
        "height": height,
        "plot_left": pad_l,
        "plot_right": width - pad_r,
        "plot_top": pad_t,
        "plot_bottom": height - pad_b,
        "y_ticks": ticks,
        "x_ticks": x_ticks,
        "month_bands": month_bands,
        "month_tooltips": month_tooltips,
        "series": series_rows,
    }


def _gcs_project_id() -> str:
    return os.environ.get("GCP_PROJECT_ID", "").strip()


def _gcs_credentials_info():
    raw = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "").strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _gcs_env_configured() -> bool:
    return bool(_gcs_bucket_name() and _gcs_project_id() and _gcs_credentials_info())


def _gcs_storage_module():
    """Railway 등에서 google-cloud-storage 미설치·네임스페이스 충돌 시에도 앱 기동을 위해 지연 로딩."""
    global _gcs_storage_loaded, _gcs_storage_mod
    if _gcs_storage_loaded:
        return _gcs_storage_mod
    _gcs_storage_loaded = True
    try:
        from google.cloud import storage as sm

        _gcs_storage_mod = sm
    except ImportError as e:
        _gcs_storage_mod = None
        logger.warning("google-cloud-storage 로드 실패: %s", e)
    return _gcs_storage_mod


def gcs_enabled() -> bool:
    if not _gcs_env_configured():
        return False
    return _gcs_storage_module() is not None


def _is_railway_deploy() -> bool:
    return bool(os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RAILWAY_PROJECT_ID"))


def _publish_allow_local_only() -> bool:
    """True면 Railway에서도 GCS 없이 로컬 디스크에만 공개 저장(휘발). 테스트용."""
    return os.environ.get("ALLOW_PUBLISH_WITHOUT_GCS", "").strip().lower() in ("1", "true", "yes")


def _get_gcs_client():
    global _gcs_client
    if _gcs_client is not None:
        return _gcs_client
    sm = _gcs_storage_module()
    if not sm:
        return None
    creds = _gcs_credentials_info()
    if not creds:
        return None
    _gcs_client = sm.Client.from_service_account_info(creds, project=_gcs_project_id() or None)
    return _gcs_client


def _gcs_bucket():
    client = _get_gcs_client()
    if not client or not _gcs_bucket_name():
        return None
    return client.bucket(_gcs_bucket_name())


def _gcs_blob_exists(blob_name: str) -> bool:
    bucket = _gcs_bucket()
    if not bucket:
        return False
    return bucket.blob(blob_name).exists()


def _gcs_upload_file(local_path: Path, blob_name: str, content_type: str | None = None):
    bucket = _gcs_bucket()
    if not bucket:
        raise RuntimeError("GCS가 설정되지 않았습니다.")
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_path), content_type=content_type)


def _gcs_upload_text(text: str, blob_name: str, content_type: str = "application/json; charset=utf-8"):
    bucket = _gcs_bucket()
    if not bucket:
        raise RuntimeError("GCS가 설정되지 않았습니다.")
    blob = bucket.blob(blob_name)
    blob.upload_from_string(text, content_type=content_type)


def _gcs_download_file(blob_name: str, local_path: Path) -> bool:
    bucket = _gcs_bucket()
    if not bucket:
        return False
    blob = bucket.blob(blob_name)
    if not blob.exists():
        return False
    local_path.parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(str(local_path))
    return True


def _published_blob_name(filename: str) -> str:
    return f"published/{filename}"


def _fm_roster_blob_name() -> str:
    return f"metadata/{FM_ROSTER_FILENAME}"


def _fm_upload_meta_blob_name() -> str:
    return "metadata/fm_upload_meta.json"


def _overtime_status_blob_name() -> str:
    return f"metadata/{OVERTIME_STATUS_FILENAME}"


def _overtime_status_meta_blob_name() -> str:
    return f"metadata/{OVERTIME_STATUS_META_FILENAME}"


def _dashboard_cache_blob_name() -> str:
    return f"metadata/{DASHBOARD_CACHE_FILENAME}"


def _permissions_blob_name() -> str:
    # 별도 폴더(access-control)로 분리 관리
    return f"access-control/{PERMISSIONS_FILENAME}"


def _fm_roster_exists_remote_or_local() -> bool:
    if gcs_enabled():
        return _gcs_blob_exists(_fm_roster_blob_name())
    return FM_ROSTER_LOCAL_PATH.is_file()


def _published_exists() -> bool:
    if gcs_enabled():
        return _gcs_blob_exists(_published_blob_name("payroll_result.csv"))
    return (PUBLISHED_DIR / "payroll_result.csv").exists()


def _download_published_to_dir(target_dir: Path) -> bool:
    if gcs_enabled():
        ok = False
        for name in PUBLISHED_FILES:
            ok = _gcs_download_file(_published_blob_name(name), target_dir / name) or ok
        return ok and (target_dir / "payroll_result.csv").exists()
    if not PUBLISHED_DIR.exists():
        return False
    target_dir.mkdir(parents=True, exist_ok=True)
    for name in PUBLISHED_FILES:
        src = PUBLISHED_DIR / name
        if src.exists():
            shutil.copy2(src, target_dir / name)
    return (target_dir / "payroll_result.csv").exists()


def _attach_fm_roster_to_dir(target_dir: Path) -> bool:
    """FM 기본정보 xlsx를 target_dir / fm_roster.xlsx 로 둠 (GCS 우선, 없으면 로컬 output/metadata)."""
    dest = target_dir / FM_ROSTER_FILENAME
    if gcs_enabled():
        if _gcs_download_file(_fm_roster_blob_name(), dest):
            return True
    if FM_ROSTER_LOCAL_PATH.is_file():
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(FM_ROSTER_LOCAL_PATH, dest)
        return True
    return False


def _attach_overtime_status_to_dir(target_dir: Path) -> bool:
    """연장근무 현황 CSV를 target_dir / overtime_status.csv 로 둠 (GCS 우선, 없으면 로컬 output/metadata)."""
    dest = target_dir / OVERTIME_STATUS_FILENAME
    if gcs_enabled():
        if _gcs_download_file(_overtime_status_blob_name(), dest):
            return True
    if OVERTIME_STATUS_LOCAL_PATH.is_file():
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(OVERTIME_STATUS_LOCAL_PATH, dest)
        return True
    return False


def _attach_dashboard_cache_to_dir(target_dir: Path) -> bool:
    dest = target_dir / DASHBOARD_CACHE_FILENAME
    if gcs_enabled():
        if _gcs_download_file(_dashboard_cache_blob_name(), dest):
            return True
    if DASHBOARD_CACHE_LOCAL_PATH.is_file():
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(DASHBOARD_CACHE_LOCAL_PATH, dest)
        return True
    return False


def _default_work_month_from_published_dir(output_dir: Path) -> str:
    """공개 급여 데이터에서 기본 근로월(YYYY-MM)을 추정한다."""
    import pandas as pd

    daily = _safe_read_csv(output_dir / "daily_summary.csv")
    if daily.empty or "date" not in daily.columns:
        return datetime.now(KST).strftime("%Y-%m")
    ds = pd.to_datetime(daily["date"], errors="coerce")
    ds = ds.dropna()
    if ds.empty:
        return datetime.now(KST).strftime("%Y-%m")
    try:
        from payroll_calculator import _infer_payroll_period

        ps, pe = _infer_payroll_period(pd.DataFrame({"date": ds}))
        return pe.strftime("%Y-%m")
    except Exception:
        pass
    return ds.max().strftime("%Y-%m")


def _normalize_work_month(raw: str | None, fallback: str) -> str:
    s = (raw or "").strip()
    if len(s) == 7 and s[4] == "-":
        try:
            y = int(s[:4])
            m = int(s[5:7])
            if 1 <= m <= 12 and 2000 <= y <= 2100:
                return f"{y:04d}-{m:02d}"
        except ValueError:
            pass
    return fallback


def _work_month_date_keys(work_month_key: str) -> list[str]:
    """근로월 기준 31일 시퀀스: 전월 25일부터 31일."""
    y = int(work_month_key[:4])
    m = int(work_month_key[5:7])
    start_month = m - 1
    start_year = y
    if start_month <= 0:
        start_month = 12
        start_year -= 1
    start = date(start_year, start_month, 25)
    return [(start + timedelta(days=i)).isoformat() for i in range(31)]


def _load_overtime_long_map(path: Path, work_month_key: str) -> dict[str, dict]:
    """CSV(long format)에서 선택 근로월 데이터를 row-key 맵으로 로드."""
    import pandas as pd

    saved_map: dict[str, dict] = {}
    saved = _safe_read_csv(path)
    if saved.empty:
        return saved_map
    saved.columns = [str(c).strip().lstrip("\ufeff") for c in saved.columns]

    # 최신 long 포맷: work_month, role, display_name, date, value
    long_cols = {"work_month", "role", "display_name", "date", "value"}
    if long_cols.issubset(set(saved.columns)):
        s = saved[saved["work_month"].astype(str).str.strip() == work_month_key]
        for _, r in s.iterrows():
            role = str(r.get("role", "")).strip()
            display_name = str(r.get("display_name", "")).strip()
            d = str(r.get("date", "")).strip()[:10]
            if not role or not display_name or not d:
                continue
            row_key = f"{role}|{display_name}"
            saved_map.setdefault(row_key, {})[d] = str(r.get("value", "")).strip()
        return saved_map

    # 구버전 wide 포맷(role/display_name + date columns) 호환: fallback 근로월에만 표시
    if "role" in saved.columns and "display_name" in saved.columns:
        date_keys = _work_month_date_keys(work_month_key)
        for _, r in saved.iterrows():
            role = str(r.get("role", "")).strip()
            display_name = str(r.get("display_name", "")).strip()
            if not role or not display_name:
                continue
            row_key = f"{role}|{display_name}"
            row_data = {}
            for d in date_keys:
                v = r.get(d, "")
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    row_data[d] = ""
                else:
                    row_data[d] = str(v).strip()
            saved_map[row_key] = row_data
    return saved_map


def _load_fm_role_name_rows(path: Path) -> list[dict]:
    """FM 기본정보에서 역할/닉네임(이름) 조합 행 목록을 생성한다."""
    import pandas as pd

    if not path.is_file():
        return []
    try:
        df = pd.read_excel(path, sheet_name=0, header=0, engine="openpyxl")
    except Exception:
        try:
            df = pd.read_excel(path, sheet_name=0, header=0)
        except Exception:
            return []
    df.columns = [str(c).strip() for c in df.columns]
    if "역할" not in df.columns:
        return []

    name_col = None
    for c in ("이름", "성명", "직원명", "한글명"):
        if c in df.columns:
            name_col = c
            break
    nickname_col = "닉네임" if "닉네임" in df.columns else None

    rows: list[dict] = []
    seen: set[str] = set()
    for _, row in df.iterrows():
        role = str(row.get("역할", "")).strip()
        if not role:
            continue
        nick = _normalize_fm_person_name(row.get(nickname_col)) if nickname_col else ""
        name = _normalize_fm_person_name(row.get(name_col)) if name_col else ""
        display_name = nick or name
        if not display_name:
            continue
        if nick and name and nick != name:
            display_name = f"{nick}({name})"
        key = f"{role}|{display_name}"
        if key in seen:
            continue
        seen.add(key)
        rows.append({"role": role, "display_name": display_name})
    role_order = ("태깅", "클리닝", "촬영", "포장", "물류")

    def _role_rank(role: str) -> int:
        r = (role or "").strip().lower()
        for i, label in enumerate(role_order):
            if label.lower() in r:
                return i
        return len(role_order)

    rows.sort(key=lambda x: (_role_rank(x["role"]), x["role"], x["display_name"]))
    return rows


def _build_overtime_status_table(output_dir: Path, work_month_key: str) -> tuple[list[dict], list[str], bool]:
    """
    연장근무 현황 테이블 행/일자컬럼 생성.
    반환: (rows, date_keys, has_saved_file)
    """
    date_keys = _work_month_date_keys(work_month_key)
    roster_rows = _load_fm_role_name_rows(output_dir / FM_ROSTER_FILENAME)
    has_saved = _attach_overtime_status_to_dir(output_dir)
    saved_map = _load_overtime_long_map(output_dir / OVERTIME_STATUS_FILENAME, work_month_key)

    rows: list[dict] = []
    for r in roster_rows:
        key = f"{r['role']}|{r['display_name']}"
        row = {"role": r["role"], "display_name": r["display_name"]}
        prev = saved_map.get(key, {})
        for d in date_keys:
            row[d] = prev.get(d, "")
        rows.append(row)
    return rows, date_keys, has_saved


def _read_published_meta_dict() -> dict | None:
    """로컬 published/meta.json 또는 GCS published/meta.json."""
    p = PUBLISHED_DIR / PUBLISHED_META_FILENAME
    if p.is_file():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    if gcs_enabled():
        import tempfile

        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tf:
            tfp = Path(tf.name)
        try:
            if _gcs_download_file(_published_blob_name(PUBLISHED_META_FILENAME), tfp):
                return json.loads(tfp.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
        finally:
            tfp.unlink(missing_ok=True)
    return None


def _write_published_meta_local(meta: dict) -> None:
    PUBLISHED_DIR.mkdir(parents=True, exist_ok=True)
    (PUBLISHED_DIR / PUBLISHED_META_FILENAME).write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _read_overtime_status_meta_dict() -> dict | None:
    p = OVERTIME_STATUS_META_LOCAL_PATH
    if p.is_file():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    if gcs_enabled():
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tf:
            tfp = Path(tf.name)
        try:
            if _gcs_download_file(_overtime_status_meta_blob_name(), tfp):
                return json.loads(tfp.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
        finally:
            tfp.unlink(missing_ok=True)
    return None


def _read_dashboard_cache_dict() -> dict | None:
    p = DASHBOARD_CACHE_LOCAL_PATH
    if p.is_file():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    if gcs_enabled():
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tf:
            tfp = Path(tf.name)
        try:
            if _gcs_download_file(_dashboard_cache_blob_name(), tfp):
                return json.loads(tfp.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
        finally:
            tfp.unlink(missing_ok=True)
    return None


def _view_cache_get(key: str, token: str):
    now = time.time()
    item = _VIEW_CACHE.get(key)
    if not item:
        return None
    if item.get("token") != token:
        return None
    if float(item.get("exp", 0)) < now:
        _VIEW_CACHE.pop(key, None)
        return None
    return item.get("val")


def _view_cache_set(key: str, token: str, val):
    _VIEW_CACHE[key] = {"token": token, "exp": (time.time() + max(_VIEW_CACHE_TTL_SEC, 1)), "val": val}


def _view_cache_clear(prefix: str | None = None):
    if not prefix:
        _VIEW_CACHE.clear()
        return
    for k in list(_VIEW_CACHE.keys()):
        if k.startswith(prefix):
            _VIEW_CACHE.pop(k, None)


def _save_dashboard_cache(ctx: dict, source: str = "") -> None:
    payload = {
        "generated_at": datetime.now(KST).isoformat(),
        "source": source,
        "published_at": ((_read_published_meta_dict() or {}).get("published_at") or ""),
        "ctx": ctx,
    }
    FM_ROSTER_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    DASHBOARD_CACHE_LOCAL_PATH.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    if gcs_enabled():
        _gcs_upload_text(json.dumps(payload, ensure_ascii=False), _dashboard_cache_blob_name())


def _rebuild_dashboard_cache_from_dir(output_dir: Path, source: str = "") -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        for name in PUBLISHED_FILES:
            src = output_dir / name
            if src.exists():
                shutil.copy2(src, tmp_dir / name)
        _attach_fm_roster_to_dir(tmp_dir)
        ctx = _build_dashboard_context(tmp_dir)
        _save_dashboard_cache(ctx, source=source)
        _view_cache_clear("dashboard")


def _format_iso_kst_display(iso_str: str | None) -> str:
    if not iso_str or not str(iso_str).strip():
        return "—"
    try:
        s = str(iso_str).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        dt = dt.astimezone(KST)
        return dt.strftime("%Y-%m-%d %H:%M (KST)")
    except (ValueError, TypeError, OSError):
        return str(iso_str)[:19]


def _read_fm_upload_meta_dict() -> dict | None:
    if FM_UPLOAD_META_LOCAL_PATH.is_file():
        try:
            return json.loads(FM_UPLOAD_META_LOCAL_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    if gcs_enabled():
        import tempfile

        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tf:
            tfp = Path(tf.name)
        try:
            if _gcs_download_file(_fm_upload_meta_blob_name(), tfp):
                return json.loads(tfp.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
        finally:
            tfp.unlink(missing_ok=True)
    return None


def _permission_default_levels() -> dict[str, str]:
    return {
        "payroll": "view",
        "dashboard": "view",
        "overtime": "view",
        "commercialization": "view",
        "admin_data": "none",
    }


def _normalize_permission_level(raw, default: str = "none") -> str:
    v = str(raw or "").strip().lower()
    if v in PERMISSION_LEVELS:
        return v
    return default


def _clean_permission_rows(rows) -> list[dict]:
    cleaned: list[dict] = []
    seen: set[str] = set()
    if not isinstance(rows, list):
        return cleaned
    for row in rows:
        if not isinstance(row, dict):
            continue
        email = str(row.get("email") or "").strip().lower()
        if not email or "@" not in email:
            continue
        if email in seen:
            continue
        out = {"email": email}
        for scope in PERMISSION_SCOPE_KEYS:
            out[scope] = _normalize_permission_level(row.get(scope), "none")
        cleaned.append(out)
        seen.add(email)
    cleaned.sort(key=lambda x: x["email"])
    return cleaned


def _empty_permissions_config() -> dict:
    return {
        "version": 1,
        "defaults": _permission_default_levels(),
        "rows": [],
        "updated_at": "",
        "updated_by": "",
    }


def _sanitize_permissions_config(cfg: dict | None) -> dict:
    base = _empty_permissions_config()
    if not isinstance(cfg, dict):
        return base
    defaults = {}
    raw_defaults = cfg.get("defaults")
    if isinstance(raw_defaults, dict):
        for scope in PERMISSION_SCOPE_KEYS:
            defaults[scope] = _normalize_permission_level(raw_defaults.get(scope), _permission_default_levels()[scope])
    else:
        defaults = _permission_default_levels()
    base["defaults"] = defaults
    base["rows"] = _clean_permission_rows(cfg.get("rows"))
    base["updated_at"] = str(cfg.get("updated_at") or "")
    base["updated_by"] = str(cfg.get("updated_by") or "")
    return base


def _read_permissions_config_dict() -> dict:
    # GCS 우선(다중 인스턴스 실시간성), 실패 시 로컬 fallback.
    if gcs_enabled():
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tf:
            tfp = Path(tf.name)
        try:
            if _gcs_download_file(_permissions_blob_name(), tfp):
                raw = json.loads(tfp.read_text(encoding="utf-8"))
                cfg = _sanitize_permissions_config(raw)
                PERMISSIONS_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
                PERMISSIONS_LOCAL_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
                return cfg
        except (json.JSONDecodeError, OSError):
            pass
        finally:
            tfp.unlink(missing_ok=True)
    if PERMISSIONS_LOCAL_PATH.is_file():
        try:
            return _sanitize_permissions_config(json.loads(PERMISSIONS_LOCAL_PATH.read_text(encoding="utf-8")))
        except (json.JSONDecodeError, OSError):
            pass
    return _empty_permissions_config()


def _load_permissions_config(force: bool = False) -> dict:
    global _PERMISSIONS_CACHE, _PERMISSIONS_CACHE_AT
    now = time.time()
    if not force and _PERMISSIONS_CACHE is not None and (now - _PERMISSIONS_CACHE_AT) <= _PERMISSIONS_CACHE_TTL_SEC:
        return _PERMISSIONS_CACHE
    cfg = _read_permissions_config_dict()
    _PERMISSIONS_CACHE = cfg
    _PERMISSIONS_CACHE_AT = now
    return cfg


def _save_permissions_config(rows: list[dict], updated_by: str = "") -> tuple[bool, str | None]:
    cfg = _empty_permissions_config()
    cfg["rows"] = _clean_permission_rows(rows)
    cfg["updated_at"] = datetime.now(KST).isoformat()
    cfg["updated_by"] = str(updated_by or "").strip().lower()
    try:
        PERMISSIONS_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
        PERMISSIONS_LOCAL_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        if gcs_enabled():
            _gcs_upload_text(
                json.dumps(cfg, ensure_ascii=False, indent=2),
                _permissions_blob_name(),
            )
        elif _is_railway_deploy() and not _publish_allow_local_only():
            return False, (
                "Railway에서는 재배포 후에도 유지되도록 GCS 저장이 필요합니다. "
                "GCP_PROJECT_ID, GCS_BUCKET, GOOGLE_APPLICATION_CREDENTIALS_JSON을 설정해 주세요."
            )
        elif _gcs_env_configured() and not gcs_enabled():
            return False, "GCS 설정은 있으나 라이브러리를 불러오지 못했습니다. 환경을 확인해 주세요."
    except Exception as e:
        return False, f"권한 저장 실패: {e}"
    _load_permissions_config(force=True)
    return True, None


def _scope_levels_for_email(email: str) -> dict[str, str]:
    # super-admin(ADMIN_EMAILS)은 항상 전체 edit
    e = str(email or "").strip().lower()
    if e and e in _admin_email_set():
        return {scope: "edit" for scope in PERMISSION_SCOPE_KEYS}
    cfg = _load_permissions_config()
    defaults = cfg.get("defaults") if isinstance(cfg.get("defaults"), dict) else {}
    levels = {
        scope: _normalize_permission_level(
            defaults.get(scope),
            _permission_default_levels()[scope],
        )
        for scope in PERMISSION_SCOPE_KEYS
    }
    if not e:
        return levels
    for row in cfg.get("rows", []):
        if str(row.get("email") or "").strip().lower() != e:
            continue
        for scope in PERMISSION_SCOPE_KEYS:
            levels[scope] = _normalize_permission_level(row.get(scope), levels[scope])
        break
    return levels


def _current_user_scope_level(scope: str) -> str:
    if auth_disabled():
        return "edit"
    if scope not in PERMISSION_SCOPE_KEYS:
        return "none"
    email = (session.get("user_email") or "").strip().lower()
    return _scope_levels_for_email(email).get(scope, "none")


def _can_current_user(scope: str, required: str = "view") -> bool:
    need = _normalize_permission_level(required, "view")
    have = _current_user_scope_level(scope)
    return PERMISSION_LEVEL_RANK.get(have, 0) >= PERMISSION_LEVEL_RANK.get(need, 1)


def require_permission(scope: str, required: str = "view"):
    def _decorator(fn):
        @wraps(fn)
        def _wrapped(*args, **kwargs):
            if _can_current_user(scope, required):
                return fn(*args, **kwargs)
            flash("해당 화면 접근 권한이 없습니다.", "error")
            return redirect(url_for("index"))

        return _wrapped

    return _decorator


def _admin_upload_display_context() -> dict:
    """관리자 업로드 화면: 마지막 파일명·시간 표시용."""
    meta = _read_published_meta_dict()
    att = {"filename": "—", "when": "—"}
    leave = {"filename": "—", "when": "—"}
    if meta:
        an = meta.get("last_attendance_name") or meta.get("attendance_name")
        at = meta.get("last_attendance_at") or meta.get("published_at")
        if an:
            att = {"filename": str(an), "when": _format_iso_kst_display(at)}
        ln = (meta.get("last_leave_name") or "").strip()
        la = (meta.get("last_leave_at") or "").strip()
        if not ln and (meta.get("leave_name") or "").strip():
            ln = (meta.get("leave_name") or "").strip()
            la = meta.get("published_at") or ""
        if ln:
            leave = {"filename": ln, "when": _format_iso_kst_display(la or meta.get("published_at"))}
    fm = _read_fm_upload_meta_dict()
    fm_disp = {"filename": "—", "when": "—"}
    if fm and fm.get("filename"):
        fm_disp = {
            "filename": str(fm["filename"]),
            "when": _format_iso_kst_display(fm.get("uploaded_at")),
        }
    perm_cfg = _load_permissions_config()
    permission_rows = perm_cfg.get("rows", []) if isinstance(perm_cfg, dict) else []
    permission_scopes = [
        {"key": "payroll", "label": "급여 데이터"},
        {"key": "dashboard", "label": "인건비 대시보드"},
        {"key": "overtime", "label": "연장근무 현황"},
        {"key": "commercialization", "label": "상품화 인건비"},
        {"key": "admin_data", "label": "관리자 데이터"},
    ]
    return {
        "upload_last_attendance": att,
        "upload_last_leave": leave,
        "upload_last_fm": fm_disp,
        "permission_rows": permission_rows,
        "permission_scopes": permission_scopes,
        "permission_levels": list(PERMISSION_LEVELS),
        "permission_updated_at": _format_iso_kst_display(perm_cfg.get("updated_at")) if isinstance(perm_cfg, dict) else "—",
        "permission_updated_by": str((perm_cfg or {}).get("updated_by") or "—"),
    }


def _sync_run_to_gcs(run_dir: Path, input_path: Path, leave_path: Path | None = None, uploaded_by: str | None = None):
    stamp = datetime.now(KST).strftime("%Y-%m-%d_%H%M%S")
    run_prefix = f"runs/{stamp}"
    now_iso = datetime.now(KST).isoformat()
    prev = _read_published_meta_dict()
    meta = {
        "published_at": now_iso,
        "uploaded_by": uploaded_by or "",
        "run_prefix": run_prefix,
        "attendance_name": input_path.name,
        "leave_name": leave_path.name if leave_path and leave_path.exists() else "",
        "last_attendance_name": input_path.name,
        "last_attendance_at": now_iso,
    }
    if leave_path and leave_path.exists():
        meta["last_leave_name"] = leave_path.name
        meta["last_leave_at"] = now_iso
        meta["last_leave_blob"] = f"metadata/{LAST_LEAVE_LOCAL_PATH.name}"
    elif prev:
        meta["last_leave_name"] = prev.get("last_leave_name") or ""
        meta["last_leave_at"] = prev.get("last_leave_at") or ""
        meta["last_leave_blob"] = prev.get("last_leave_blob") or ""
    else:
        meta["last_leave_name"] = ""
        meta["last_leave_at"] = ""
        meta["last_leave_blob"] = ""

    if gcs_enabled():
        _gcs_upload_file(input_path, f"uploads/attendance/{stamp}{input_path.suffix}")
        if leave_path and leave_path.exists():
            _gcs_upload_file(leave_path, f"uploads/leave/{stamp}{leave_path.suffix}")
            _gcs_upload_file(leave_path, f"metadata/{LAST_LEAVE_LOCAL_PATH.name}")
        for name in PUBLISHED_FILES:
            src = run_dir / name
            if not src.exists():
                continue
            _gcs_upload_file(src, f"{run_prefix}/{name}", content_type="text/csv; charset=utf-8")
            _gcs_upload_file(src, _published_blob_name(name), content_type="text/csv; charset=utf-8")
        _gcs_upload_text(
            json.dumps(meta, ensure_ascii=False, indent=2),
            _published_blob_name(PUBLISHED_META_FILENAME),
        )
    if leave_path and leave_path.exists():
        FM_ROSTER_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
        shutil.copy2(leave_path, LAST_LEAVE_LOCAL_PATH)
    _write_published_meta_local(meta)


def _resolve_leave_path_for_upload(tmp_dir: Path, file_leave_obj) -> tuple[Path | None, str]:
    """
    관리자 업로드 시 사용할 휴가 파일 경로를 결정.
    우선순위: 이번 요청 업로드 > 로컬 보관본 > GCS 보관본/이전 업로드.
    반환: (leave_path_or_none, source_label)
    """
    if file_leave_obj and file_leave_obj.filename and file_leave_obj.filename.strip():
        if file_leave_obj.filename.lower().endswith((".xlsx", ".xls")):
            leave_path = tmp_dir / "leave.xlsx"
            file_leave_obj.save(str(leave_path))
            return leave_path, "uploaded"
        return None, "invalid"

    if LAST_LEAVE_LOCAL_PATH.is_file():
        leave_path = tmp_dir / "leave.xlsx"
        shutil.copy2(LAST_LEAVE_LOCAL_PATH, leave_path)
        return leave_path, "local_cached"

    meta = _read_published_meta_dict() or {}
    if not gcs_enabled():
        return None, "none"

    leave_path = tmp_dir / "leave.xlsx"
    # 1) 최신 고정 보관본(신규 정책)
    blob = (meta.get("last_leave_blob") or "").strip() or f"metadata/{LAST_LEAVE_LOCAL_PATH.name}"
    if _gcs_download_file(blob, leave_path):
        return leave_path, "gcs_cached"

    # 2) 이전 정책 업로드 경로 추정 (uploads/leave/{stamp}.{ext})
    last_at = (meta.get("last_leave_at") or "").strip()
    last_name = (meta.get("last_leave_name") or meta.get("leave_name") or "").strip()
    suffix = Path(last_name).suffix if last_name else ".xlsx"
    if suffix.lower() not in (".xlsx", ".xls"):
        suffix = ".xlsx"
    if last_at:
        try:
            dt = datetime.fromisoformat(last_at)
            stamp = dt.astimezone(KST).strftime("%Y-%m-%d_%H%M%S")
            old_blob = f"uploads/leave/{stamp}{suffix}"
            if _gcs_download_file(old_blob, leave_path):
                return leave_path, "gcs_legacy"
        except ValueError:
            pass

    return None, "none"


def _to_num(val, default=0.0):
    try:
        if val is None:
            return default
        if isinstance(val, str):
            s = val.strip().replace(",", "")
            if not s:
                return default
            return float(s)
        return float(val)
    except (TypeError, ValueError):
        return default


def _safe_read_csv(path):
    import pandas as pd
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(p, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()


def _normalize_employee_id_val(val):
    import pandas as pd

    if val is None:
        return ""
    if isinstance(val, float) and pd.isna(val):
        return ""
    s = str(val).strip().replace(",", "")
    if not s or s.lower() == "nan":
        return ""
    try:
        f = float(s)
        if abs(f - int(f)) < 1e-9:
            return str(int(f))
        return s
    except (ValueError, TypeError, OverflowError):
        return s


def _normalize_fm_person_name(val) -> str:
    """FM 목록·급여 이름 매칭용: 공백 정리."""
    import pandas as pd

    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return ""
    return " ".join(s.split())


def _fm_person_name_compact(val) -> str:
    """이름 비교용: 모든 공백 제거(‘홍 길동’ vs ‘홍길동’)."""
    n = _normalize_fm_person_name(val)
    return "".join(n.split()) if n else ""


def _payroll_display_name_keys(raw) -> list[str]:
    """
    급여 employee_name 후보 키 목록.
    예: '골드(김은영)' → 전체, 무공백, 괄호 앞 '골드', 괄호 안 '김은영' (FM 닉네임·이름과 맞추기 위함).
    """
    import re

    n = _normalize_fm_person_name(raw)
    if not n:
        return []
    keys: list[str] = [n]
    c = "".join(n.split())
    if c and c != n:
        keys.append(c)
    m = re.match(r"^(.+?)\s*[\(（]\s*([^)）]+)\s*[\)）]\s*$", n)
    if m:
        outer = m.group(1).strip()
        inner = m.group(2).strip()
        for p in (outer, inner):
            if not p:
                continue
            keys.append(p)
            pc = "".join(p.split())
            if pc and pc != p:
                keys.append(pc)
    out: list[str] = []
    seen: set[str] = set()
    for k in keys:
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _fm_name_role_lookup(name_val, fm_name_to_role: dict[str, str]) -> str | None:
    """급여 이름으로 FM 역할 조회. 표시명·괄호 분리·닉네임·정규화 키 순."""
    for key in _payroll_display_name_keys(name_val):
        if key in fm_name_to_role:
            return fm_name_to_role[key]
        nk = _normalize_fm_person_name(key)
        if nk and nk in fm_name_to_role:
            return fm_name_to_role[nk]
        ck = "".join(nk.split()) if nk else ""
        if ck and ck in fm_name_to_role:
            return fm_name_to_role[ck]
    return None


def _fm_name_to_role_dict_add(name_to_role: dict[str, str], display_name: str, role: str) -> None:
    """이름→역할 맵에 표시용·무공백 키 둘 다 등록."""
    n = _normalize_fm_person_name(display_name)
    r = (role or "").strip()
    if not n or not r:
        return
    name_to_role[n] = r
    c = "".join(n.split())
    if c and c != n:
        name_to_role[c] = r


def _load_fm_roster_data(path: Path):
    """
    FM 목록 xlsx → (사번→역할 DataFrame, 이름→역할 dict).
    - 사번이 있는 행만 eid 테이블에 포함(기존과 동일).
    - 이름 컬럼이 있으면 **모든 행**에서 이름→역할을 등록(사번이 급여와 안 맞아도 이름으로 보조 매칭).
    - 닉네임 컬럼이 있으면 닉네임→역할도 등록(급여명이 '골드(김은영)' 형태일 때 괄호 앞과 맞춤).
    이름 컬럼 후보: 이름, 성명, 직원명, 한글명
    """
    import pandas as pd

    if not path.is_file():
        return None
    try:
        df = pd.read_excel(path, sheet_name=0, header=0, engine="openpyxl")
    except Exception:
        try:
            df = pd.read_excel(path, sheet_name=0, header=0)
        except Exception:
            return None
    df.columns = [str(c).strip() for c in df.columns]
    if "사번" not in df.columns or "역할" not in df.columns:
        return None

    name_col = None
    for c in ("이름", "성명", "직원명", "한글명"):
        if c in df.columns:
            name_col = c
            break

    roles = df["역할"].astype(str).str.strip()
    eid_norm = df["사번"].map(_normalize_employee_id_val)
    out = pd.DataFrame({"eid_norm": eid_norm, "fm_role": roles})
    out = out[out["eid_norm"].str.len() > 0].drop_duplicates(subset=["eid_norm"], keep="last")

    name_to_role: dict[str, str] = {}
    if name_col is not None:
        for _, row in df.iterrows():
            role = str(row.get("역할", "")).strip()
            if not role:
                continue
            _fm_name_to_role_dict_add(name_to_role, row.get(name_col), role)

    if "닉네임" in df.columns:
        for _, row in df.iterrows():
            role = str(row.get("역할", "")).strip()
            if not role:
                continue
            _fm_name_to_role_dict_add(name_to_role, row.get("닉네임"), role)

    if out.empty and not name_to_role:
        return None
    return out, name_to_role


def _load_fm_roster_pairs(path: Path):
    """FM 목록 xlsx에서 (사번, 역할) 정규화 테이블. 이름만 있는 행은 빈 DataFrame + 별도 매핑으로 처리."""
    import pandas as pd

    r = _load_fm_roster_data(path)
    if r is None:
        return None
    pairs, name_map = r
    if pairs.empty and not name_map:
        return None
    if pairs.empty:
        return pd.DataFrame(columns=["eid_norm", "fm_role"])
    return pairs


def _build_dashboard_context(output_dir: Path):
    import pandas as pd

    daily = _safe_read_csv(output_dir / "daily_summary.csv")
    payroll = _safe_read_csv(output_dir / "payroll_result.csv")
    anomaly = _safe_read_csv(output_dir / "anomaly_report.csv")

    if daily.empty or payroll.empty:
        return {"dashboard_ready": False}

    daily.columns = [str(c).strip().lstrip("\ufeff") for c in daily.columns]
    payroll.columns = [str(c).strip().lstrip("\ufeff") for c in payroll.columns]
    anomaly.columns = [str(c).strip().lstrip("\ufeff") for c in anomaly.columns] if not anomaly.empty else []

    if "date" not in daily.columns:
        return {"dashboard_ready": False}

    daily["date"] = pd.to_datetime(daily["date"], errors="coerce")
    daily = daily[daily["date"].notna()].copy()
    if daily.empty:
        return {"dashboard_ready": False}

    # 급여 화면의 '주휴용'(산정기간 이전) 일자는 참고용이므로, 인건비 대시보드 일자·근무시간 집계에서 제외
    payroll_period_start = ""
    payroll_period_end = ""
    try:
        from payroll_calculator import _infer_payroll_period

        ps, pe = _infer_payroll_period(daily)
        payroll_period_start = ps.isoformat()
        payroll_period_end = pe.isoformat()
        dpart = daily["date"].dt.normalize().dt.date
        daily = daily.loc[(dpart >= ps) & (dpart <= pe)].copy()
    except Exception:
        logger.exception("dashboard: 급여 산정기간 필터 생략(전체 daily 사용)")
    if daily.empty:
        return {"dashboard_ready": False}

    holiday_dates = set()
    try:
        from leave_merger import get_weekday_public_holidays_kr
        d_min = daily["date"].min().date()
        d_max = daily["date"].max().date()
        holiday_dates = get_weekday_public_holidays_kr(d_min, d_max)
    except Exception:
        pass

    try:
        from payroll_calculator import calc_daily
        costs = daily.apply(lambda r: calc_daily(r, holiday_dates=holiday_dates), axis=1, result_type="expand")
        daily["base_pay_calc"] = costs[0].astype(float)
        daily["ot_pay_calc"] = costs[1].astype(float)
    except Exception:
        mins = daily.get("net_minutes", 0).fillna(0).astype(float)
        base_min = mins.clip(upper=480)
        ot_min = (mins - 480).clip(lower=0)
        daily["base_pay_calc"] = base_min / 60 * _RECALC_HOURLY
        daily["ot_pay_calc"] = ot_min / 60 * _RECALC_HOURLY * _RECALC_OT_MULT

    daily["daily_cost"] = daily["base_pay_calc"] + daily["ot_pay_calc"]
    daily["date_key"] = daily["date"].dt.strftime("%Y-%m-%d")
    if "employee_name" not in daily.columns:
        daily["employee_name"] = daily.get("employee_id", "").astype(str)

    total_pay_series = payroll["total_pay"] if "total_pay" in payroll.columns else pd.Series(dtype=float)
    total_pay = int(round(total_pay_series.apply(_to_num).sum()))
    total_employees = int(payroll.get("employee_id", pd.Series(dtype=float)).nunique())
    total_work_minutes = int(round(daily.get("net_minutes", pd.Series(dtype=float)).fillna(0).astype(float).sum()))
    total_work_hours = round(total_work_minutes / 60, 1)

    daily_cost = (
        daily.groupby("date_key", as_index=False)["daily_cost"]
        .sum()
        .sort_values("date_key")
    )
    work_by_date: dict[str, float] = {
        str(r["date_key"]): float(r["daily_cost"]) for _, r in daily_cost.iterrows()
    }

    # 주휴수당: weekly_allowance_result → 주휴일(일요일) 행으로 합산 (급여 산정기간 내 일요일만)
    wa_by_sunday: dict[str, float] = {}
    wdf_for_order = pd.DataFrame()
    try:
        from payroll_calculator import HOURLY_WAGE, _infer_payroll_period, _week_sunday

        ps, pe = _infer_payroll_period(daily)
        wa_path = output_dir / "weekly_allowance_result.csv"
        if wa_path.exists():
            wdf_for_order = pd.read_csv(wa_path, encoding="utf-8-sig")
            wdf_for_order.columns = [str(c).strip().lstrip("\ufeff") for c in wdf_for_order.columns]
            if "week_start" in wdf_for_order.columns and "weekly_allowance_minutes" in wdf_for_order.columns:
                tmp: dict[str, float] = {}
                for _, wrow in wdf_for_order.iterrows():
                    try:
                        sun = _week_sunday(wrow["week_start"])
                    except Exception:
                        continue
                    if not (ps <= sun <= pe):
                        continue
                    m = int(round(float(wrow.get("weekly_allowance_minutes") or 0)))
                    if m <= 0:
                        continue
                    piece = round(round(m / 60.0, 1) * HOURLY_WAGE, 0)
                    sk = sun.isoformat()
                    tmp[sk] = tmp.get(sk, 0.0) + piece
                wa_by_sunday = tmp
    except Exception:
        logger.exception("dashboard: 주휴 일자별 집계 생략")

    def _dash_fmt_md(dk: str) -> str:
        d = pd.to_datetime(dk)
        return f"{d.month:02d}/{d.day:02d}"

    def _dash_weekly_bounds(cols_order: list, weekly_key: str) -> tuple[str | None, str | None]:
        idx = next((i for i, (kk, _) in enumerate(cols_order) if kk == weekly_key), None)
        if idx is None:
            return None, None
        prev_d = None
        for i in range(idx - 1, -1, -1):
            kk = cols_order[i][0]
            if isinstance(kk, str) and len(kk) == 10 and kk[4] == "-" and kk[7] == "-":
                prev_d = kk
                break
        next_d = None
        for i in range(idx + 1, len(cols_order)):
            kk = cols_order[i][0]
            if isinstance(kk, str) and len(kk) == 10 and kk[4] == "-" and kk[7] == "-":
                next_d = kk
                break
        return prev_d, next_d

    def _dash_weekly_label(name: str, ws_mon: str, prev_d: str | None, next_d: str | None) -> str:
        from datetime import timedelta

        wdt = pd.to_datetime(ws_mon)
        if prev_d is None:
            prev_d = (wdt + timedelta(days=4)).strftime("%Y-%m-%d")
        if next_d is None:
            next_d = (wdt + timedelta(days=7)).strftime("%Y-%m-%d")
        return f"{name} ({_dash_fmt_md(prev_d)}~{_dash_fmt_md(next_d)})"

    chart_rows: list[dict] = []
    try:
        from payroll_calculator import _week_sunday, build_payroll_column_order

        wdf_cols = wdf_for_order
        if wdf_cols.empty or "week_start" not in wdf_cols.columns:
            wdf_cols = pd.DataFrame()
        cols_order, col_to_week = build_payroll_column_order(daily, wdf_cols)
        if cols_order:
            seen_work: set[str] = set()
            items: list[tuple[str, int, dict]] = []
            for k, _h in cols_order:
                kk = str(k)
                if len(kk) == 10 and kk[4] == "-" and kk[7] == "-":
                    w_amt = work_by_date.get(kk, 0.0)
                    if w_amt > 0:
                        items.append(
                            (kk, 0, {"date_key": kk, "display_label": None, "amount": int(round(w_amt)), "kind": "work"})
                        )
                        seen_work.add(kk)
                elif kk.startswith("주휴"):
                    ws_mon = col_to_week.get(k)
                    if not ws_mon:
                        ws_mon = col_to_week.get(kk)
                    if not ws_mon:
                        continue
                    sun = _week_sunday(ws_mon)
                    sk = sun.isoformat() if hasattr(sun, "isoformat") else str(sun)
                    wa_amt = wa_by_sunday.get(sk, 0.0)
                    if wa_amt <= 0:
                        continue
                    p_d, n_d = _dash_weekly_bounds(cols_order, k)
                    lbl = _dash_weekly_label(kk, ws_mon, p_d, n_d)
                    sort_key = p_d if p_d else sk
                    items.append(
                        (
                            sort_key,
                            1,
                            {"date_key": sk, "display_label": lbl, "amount": int(round(wa_amt)), "kind": "weekly"},
                        )
                    )
            for dk in sorted(work_by_date.keys()):
                if dk in seen_work:
                    continue
                w_amt = work_by_date.get(dk, 0.0)
                if w_amt > 0:
                    items.append((dk, 0, {"date_key": dk, "display_label": None, "amount": int(round(w_amt)), "kind": "work"}))
            items.sort(key=lambda x: (x[0], x[1]))
            chart_rows = [t[2] for t in items]
    except Exception:
        logger.exception("dashboard: 급여 열 순서 기반 일자별 추이 실패, 단순 정렬로 대체")
        chart_rows = []

    all_day_keys = sorted(set(work_by_date.keys()) | set(wa_by_sunday.keys()))
    if not chart_rows:
        for dk in all_day_keys:
            w_amt = work_by_date.get(dk, 0.0)
            if w_amt > 0:
                chart_rows.append(
                    {"date_key": dk, "display_label": None, "amount": int(round(w_amt)), "kind": "work"}
                )
            wa_amt = wa_by_sunday.get(dk, 0.0)
            if wa_amt > 0:
                chart_rows.append(
                    {"date_key": dk, "display_label": None, "amount": int(round(wa_amt)), "kind": "weekly"}
                )
    chart_max = max((r["amount"] for r in chart_rows), default=1)

    # 주간 트래킹(월요일 시작): 일자 합산(cost)을 week_start(월요일) 기준으로 재집계
    daily["week_start"] = (daily["date"] - pd.to_timedelta(daily["date"].dt.weekday, unit="D")).dt.normalize()
    daily["week_start_key"] = daily["week_start"].dt.strftime("%Y-%m-%d")
    weekly_cost = (
        daily.groupby("week_start_key", as_index=False)["daily_cost"]
        .sum()
        .sort_values("week_start_key")
    )

    def _fmt_week_label(week_start_key: str) -> str:
        ws = pd.to_datetime(week_start_key)
        we = ws + pd.Timedelta(days=6)
        return f"{ws.month}/{ws.day}~{we.month}/{we.day}"

    weekly_labels = [(_fmt_week_label(ws),) for ws in weekly_cost["week_start_key"].tolist()]
    weekly_chart_labels = [x[0] for x in weekly_labels]
    weekly_chart_values = [round(float(v), 0) for v in weekly_cost["daily_cost"].tolist()]

    total_pay_num = payroll["total_pay"].apply(_to_num) if "total_pay" in payroll.columns else pd.Series([0] * len(payroll))
    top_employees = (
        payroll.assign(total_pay_num=total_pay_num)
        [["employee_name", "employee_id", "total_pay_num"]]
        .sort_values("total_pay_num", ascending=False)
        .head(10)
    )
    top_employee_rows = [
        {
            "employee_name": str(r["employee_name"]),
            "employee_id": str(r["employee_id"]),
            "total_pay": int(round(float(r["total_pay_num"]))),
        }
        for _, r in top_employees.iterrows()
    ]

    daily_rank = (
        daily.groupby("employee_name", as_index=False)["daily_cost"]
        .sum()
        .sort_values("daily_cost", ascending=False)
        .head(10)
    )
    daily_rank_rows = [
        {"employee_name": str(r["employee_name"]), "cost": int(round(float(r["daily_cost"])))}
        for _, r in daily_rank.iterrows()
    ]

    anomaly_count = int(len(anomaly)) if hasattr(anomaly, "__len__") else 0
    first_date = all_day_keys[0] if all_day_keys else "-"
    last_date = all_day_keys[-1] if all_day_keys else "-"

    fm_path = output_dir / FM_ROSTER_FILENAME
    fm_roster_data = _load_fm_roster_data(fm_path)
    fm_roster_ready = fm_roster_data is not None
    fm_role_rows: list[dict] = []
    fm_matched_in_payroll = 0
    fm_payroll_rows = int(len(payroll))
    if fm_roster_ready and "employee_id" in payroll.columns and "total_pay" in payroll.columns:
        fm_pairs, fm_name_to_role = fm_roster_data
        if fm_pairs.empty:
            fm_pairs = pd.DataFrame(columns=["eid_norm", "fm_role"])
        pm = payroll.copy()
        pm["eid_norm"] = pm["employee_id"].map(_normalize_employee_id_val)
        if "employee_name" in pm.columns:
            pm["name_norm"] = pm["employee_name"].map(_normalize_fm_person_name)
        else:
            pm["name_norm"] = ""
        merged = pm.merge(fm_pairs, on="eid_norm", how="left")
        if fm_name_to_role:
            miss = merged["fm_role"].isna()
            if "employee_name" in merged.columns:
                miss = miss & merged["employee_name"].notna()
                merged.loc[miss, "fm_role"] = merged.loc[miss].apply(
                    lambda r: _fm_name_role_lookup(r.get("employee_name"), fm_name_to_role),
                    axis=1,
                )
            else:
                miss = miss & (merged["name_norm"].str.len() > 0)
                merged.loc[miss, "fm_role"] = merged.loc[miss, "name_norm"].map(fm_name_to_role)
        fm_matched_in_payroll = int(merged["fm_role"].notna().sum())
        merged["fm_role"] = merged["fm_role"].fillna("(FM 목록 없음)")
        tpm = merged["total_pay"].apply(_to_num)
        role_grp = (
            merged.assign(total_pay_num=tpm)
            .groupby("fm_role", as_index=False)
            .agg(headcount=("employee_id", "nunique"), total_pay=("total_pay_num", "sum"))
            .sort_values("total_pay", ascending=False)
        )
        fm_role_rows = [
            {
                "role": str(r["fm_role"]),
                "headcount": int(r["headcount"]),
                "total_pay": int(round(float(r["total_pay"]))),
            }
            for _, r in role_grp.iterrows()
        ]

    return {
        "dashboard_ready": True,
        "kpi_total_pay": total_pay,
        "kpi_total_employees": total_employees,
        "kpi_total_work_hours": total_work_hours,
        "kpi_anomaly_count": anomaly_count,
        "payroll_period_start": payroll_period_start,
        "payroll_period_end": payroll_period_end,
        "period_start": first_date,
        "period_end": last_date,
        "chart_rows": chart_rows,
        "chart_max": chart_max,
        "weekly_chart_labels": weekly_chart_labels,
        "weekly_chart_values": weekly_chart_values,
        "top_employee_rows": top_employee_rows,
        "daily_rank_rows": daily_rank_rows,
        "fm_roster_ready": fm_roster_ready,
        "fm_role_rows": fm_role_rows,
        "fm_matched_in_payroll": fm_matched_in_payroll,
        "fm_payroll_rows": fm_payroll_rows,
    }


@app.route("/overtime-status")
def overtime_status():
    if not _can_current_user("overtime", "view"):
        flash("연장근무 현황 조회 권한이 없습니다.", "error")
        return redirect(url_for("index"))
    default_month = datetime.now(KST).strftime("%Y-%m")
    work_month = _normalize_work_month(request.args.get("work_month"), default_month)
    can_save = _can_current_user("overtime", "edit")
    fm_meta = _read_fm_upload_meta_dict() or {}
    ov_meta = _read_overtime_status_meta_dict() or {}
    token = f"{work_month}|{fm_meta.get('uploaded_at','')}|{ov_meta.get('saved_at','')}|{int(can_save)}"
    hit = _view_cache_get(f"overtime:{work_month}", token)
    if hit is not None:
        return render_template("overtime_status.html", **hit)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        _attach_fm_roster_to_dir(tmp_dir)
        _attach_overtime_status_to_dir(tmp_dir)
        rows, date_keys, has_saved = _build_overtime_status_table(tmp_dir, work_month)
        if not rows:
            ctx = {
                "overtime_ready": False,
                "overtime_error": "FM 기본정보(역할 + 닉네임/이름)가 없어 테이블을 만들 수 없습니다.",
                "date_keys": [],
                "date_headers": [],
                "rows": [],
                "can_save": False,
                "save_url": None,
                "has_saved": has_saved,
                "work_month": work_month,
                "period_start": "",
                "period_end": "",
            }
            _view_cache_set(f"overtime:{work_month}", token, ctx)
            return render_template("overtime_status.html", **ctx)
        period_start = date_keys[0]
        period_end = date_keys[-1]
        date_headers = [
            {
                "key": d,
                "label": f"{int(d[5:7]):02d}.{int(d[8:10]):02d}({('월', '화', '수', '목', '금', '토', '일')[datetime.fromisoformat(d).weekday()]})",
                "dow": ("월", "화", "수", "목", "금", "토", "일")[datetime.fromisoformat(d).weekday()],
                "is_weekend": datetime.fromisoformat(d).weekday() >= 5,
            }
            for d in date_keys
        ]
        ctx = {
            "overtime_ready": True,
            "overtime_error": None,
            "date_keys": date_keys,
            "date_headers": date_headers,
            "rows": rows,
            "work_month": work_month,
            "period_start": period_start,
            "period_end": period_end,
            "can_save": can_save,
            "save_url": (url_for("save_overtime_status") if can_save else None),
            "has_saved": has_saved,
        }
        _view_cache_set(f"overtime:{work_month}", token, ctx)
        return render_template("overtime_status.html", **ctx)


def _is_non_half_hour(val) -> bool:
    """근무시간 소수가 .0 또는 .5가 아니면 True (주휴 제외 일자 컬럼용)."""
    if val is None or val == "":
        return False
    try:
        f = float(val)
        frac = f - int(f)
        return frac > 0.05 and abs(frac - 0.5) > 0.05
    except (TypeError, ValueError):
        return False


app.jinja_env.filters["is_non_half_hour"] = _is_non_half_hour

# 일자 컬럼 기준 급여 재계산용 상수 (payroll_calculator와 동일)
_RECALC_HOURLY = 11_000
_RECALC_OT_MULT = 1.5
_RECALC_DAILY_CAP_MIN = 8 * 60


def _recalc_pay_from_date_columns(df, contract_types=None, employee_contracts=None, holiday_dates=None, date_col_to_date=None):
    """
    DataFrame의 일자 컬럼(숫자 시간)만으로 base_pay, overtime_pay, total_pay 등을 재계산해 덮어쓴다.
    자동채우기·셀 수정 후 내보내기 시 합계가 맞도록 한다.
    주휴용(산정기간 이전 일자) 컬럼은 기본급·야근에서 제외한다.
    _contract_override가 있으면 해당 행은 그 계약의 소정근로시간을 일일 cap으로 사용(사번없음 계약 선택 반영).
    holiday_dates·date_col_to_date가 있으면 프리랜서의 명절(공휴일) 근무는 전부 야근수당(1.5배)으로 반영.
    """
    pay_cols = {"base_pay", "overtime_pay", "overtime_hours", "weekly_allowance_pay", "weekly_allowance_hours", "unpaid_hours", "total_pay"}
    def _is_payroll_date_col(c):
        """기본급·야근 재계산에 넣을 일자 컬럼만 True. 주휴1 등·주휴용(산정기간 이전)은 제외."""
        s = str(c).replace("\r\n", "\n").replace("\r", "\n")
        if c in ("employee_id", "employee_name", "_contract_override") or c in pay_cols:
            return False
        if s.strip().startswith("주휴"):
            return False
        if "주휴용" in s:
            return False
        return True

    date_cols = [c for c in df.columns if _is_payroll_date_col(c)]
    if not date_cols:
        return
    use_holiday = bool(holiday_dates and date_col_to_date)
    for i in df.index:
        cap_min = _RECALC_DAILY_CAP_MIN
        override = df.at[i, "_contract_override"] if "_contract_override" in df.columns else None
        if contract_types and override and str(override).strip():
            ctype = str(override).strip()
            defn = (contract_types or {}).get(ctype)
            if defn and "scheduled_minutes" in defn:
                cap_min = int(defn["scheduled_minutes"])
        emp = df.at[i, "employee_id"]
        emp_key = str(emp).strip()
        try:
            if isinstance(emp, (int, float)) and str(emp) != "nan":
                emp_key = str(int(emp))
        except (ValueError, TypeError):
            pass
        is_freelancer = (
            (override and str(override).strip().startswith("freelancer_"))
            or ((employee_contracts or {}).get(emp_key) or (employee_contracts or {}).get("default") or "").startswith("freelancer_")
            or str(emp).strip().upper().startswith("F")
        )
        base_total = 0.0
        ot_total = 0.0
        # 주휴 컬럼(주휴1, 주휴2, …) 합계 → 주휴수당·산정시간 (편집 반영용, payroll_calculator와 동일 시급)
        wa_hours_sum = 0.0
        for c in df.columns:
            cs = str(c).strip()
            if not cs.startswith("주휴") or "주휴용" in cs:
                continue
            if c in pay_cols:
                continue
            v = df.at[i, c]
            if v is None or (isinstance(v, float) and str(v) == "nan"):
                continue
            try:
                wa_hours_sum += float(v)
            except (TypeError, ValueError):
                pass
        wa_hours_rounded = round(wa_hours_sum, 1)
        wa_from_weekly = round(wa_hours_rounded * _RECALC_HOURLY, 0)

        for c in date_cols:
            v = df.at[i, c]
            if v is None or (isinstance(v, float) and str(v) == "nan"):
                continue
            try:
                h = float(v)
            except (TypeError, ValueError):
                continue
            mins = h * 60
            if use_holiday and is_freelancer and c in date_col_to_date and date_col_to_date[c] in holiday_dates:
                base_min = 0
                ot_min = mins
            else:
                base_min = min(mins, cap_min)
                ot_min = max(mins - cap_min, 0)
            base_total += base_min / 60 * _RECALC_HOURLY
            ot_total += ot_min / 60 * _RECALC_HOURLY * _RECALC_OT_MULT
        # 주휴 컬럼(주휴1, …)이 있으면 합계×시급으로 주휴수당·산정시간 덮어씀 (편집·구글시트 보내기 일치)
        has_weekly_cols = any(
            str(c).strip().startswith("주휴") and "주휴용" not in str(c) and c not in pay_cols
            for c in df.columns
        )
        if has_weekly_cols:
            wa = wa_from_weekly
            df.at[i, "weekly_allowance_pay"] = int(wa)
            df.at[i, "weekly_allowance_hours"] = wa_hours_rounded
        else:
            wa = df.at[i, "weekly_allowance_pay"]
            try:
                wa = 0.0 if wa is None or (isinstance(wa, float) and str(wa) == "nan") else float(wa)
            except (TypeError, ValueError):
                wa = 0.0
            df.at[i, "weekly_allowance_pay"] = round(wa, 0)
        df.at[i, "base_pay"] = round(base_total, 0)
        df.at[i, "overtime_pay"] = round(ot_total, 0)
        df.at[i, "overtime_hours"] = round(ot_total / (_RECALC_HOURLY * _RECALC_OT_MULT), 1)
        df.at[i, "total_pay"] = round(base_total + ot_total + wa, 0)




def _admin_email_set():
    raw = os.environ.get("ADMIN_EMAILS", "").strip()
    if not raw:
        return set()
    return {e.strip().lower() for e in raw.split(",") if e.strip()}


def is_current_user_admin():
    if auth_disabled():
        return True
    admins = _admin_email_set()
    if not admins:
        return False
    email = (session.get("user_email") or "").strip().lower()
    return email in admins


def resolve_export_output_dir():
    rid = session.get("last_run_id")
    if rid and rid != PUBLISHED_ID:
        p = OUTPUT_BASE / str(rid)
        if p.is_dir() and (p / "payroll_result.csv").exists():
            return p
    if _published_exists():
        return PUBLISHED_DIR
    return None


def _apply_browser_rows_to_payroll_csv(output_dir: Path, rows: list) -> tuple[bool, str | None]:
    """브라우저 테이블 rows(JSON)로 payroll_result.csv를 재계산해 output_dir에 기록. (export·공개 저장 공통)"""
    import pandas as pd

    if not rows:
        return False, "행 데이터가 없습니다."
    try:
        df = pd.DataFrame(rows)
        if not {"employee_id", "employee_name"}.issubset(df.columns):
            return False, "테이블 데이터에 employee_id/employee_name 컬럼이 없습니다."

        def _to_number(v):
            if v is None:
                return None
            if isinstance(v, (int, float)):
                return v
            if isinstance(v, str):
                s = v.strip()
                if not s:
                    return None
                s = s.replace(",", "")
                try:
                    return float(s)
                except ValueError:
                    return v
            return v

        for col in df.columns:
            if col in ("employee_id", "employee_name", "_contract_override"):
                continue
            df[col] = df[col].map(_to_number)

        contract_types = {}
        employee_contracts = {}
        if CONTRACT_CONFIG_PATH.exists():
            try:
                from attendance_normalizer import load_contract_config

                contract_types, employee_contracts = load_contract_config()
            except Exception:
                pass
        holiday_dates = set()
        date_col_to_date = {}
        try:
            daily_path = output_dir / "daily_summary.csv"
            if daily_path.exists():
                daily = pd.read_csv(daily_path, encoding="utf-8-sig", nrows=1)
                daily.columns = [str(c).strip().lstrip("\ufeff") for c in daily.columns]
                if "date" in daily.columns:
                    daily_full = pd.read_csv(daily_path, encoding="utf-8-sig")
                    daily_full.columns = [str(c).strip().lstrip("\ufeff") for c in daily_full.columns]
                    daily_full["date"] = pd.to_datetime(daily_full["date"]).dt.normalize()
                    from payroll_calculator import _infer_payroll_period

                    payroll_start, payroll_end = _infer_payroll_period(daily_full)
                    from leave_merger import get_weekday_public_holidays_kr

                    holiday_dates = get_weekday_public_holidays_kr(payroll_start, payroll_end)
                    pay_cols_set = {
                        "base_pay",
                        "overtime_pay",
                        "overtime_hours",
                        "weekly_allowance_pay",
                        "weekly_allowance_hours",
                        "unpaid_hours",
                        "total_pay",
                    }
                    for c in df.columns:
                        if (
                            c in ("employee_id", "employee_name", "_contract_override")
                            or c in pay_cols_set
                            or str(c).strip().startswith("주휴")
                            or "주휴용" in str(c)
                        ):
                            continue
                        part = str(c).split("\n")[0].strip()
                        if "/" in part:
                            try:
                                m, d = map(int, part.split("/", 1))
                                year = payroll_start.year if m >= payroll_start.month else payroll_start.year - 1
                                from datetime import date as date_cls

                                date_col_to_date[c] = date_cls(year, m, d)
                            except (ValueError, TypeError):
                                pass
        except Exception:
            pass
        _recalc_pay_from_date_columns(
            df,
            contract_types=contract_types,
            employee_contracts=employee_contracts,
            holiday_dates=holiday_dates,
            date_col_to_date=date_col_to_date,
        )

        df_export = df.drop(columns=["_contract_override"], errors="ignore")
        csv_path = output_dir / "payroll_result.csv"
        df_export.to_csv(csv_path, index=False, encoding="utf-8-sig")
    except Exception as e:
        return False, f"급여 데이터를 반영하지 못했습니다: {e}"
    return True, None


def _make_payroll_result_response(
    output_dir: Path,
    *,
    read_only: bool = False,
    allow_published_edit: bool = False,
    back_href: str | None = None,
    back_label: str | None = None,
):
    import pandas as pd

    if back_href is None:
        back_href = url_for("index") if read_only else url_for("admin_data")
    if back_label is None:
        back_label = "← 홈" if read_only else "← 관리자 데이터"

    def _err_template():
        if read_only:
            return render_template("public_home.html")
        return render_template("upload.html", **_admin_upload_display_context())

    try:
        def safe_read_csv(path):
            p = Path(path)
            if not p.exists():
                return pd.DataFrame()
            if p.stat().st_size == 0:
                return pd.DataFrame()
            try:
                return pd.read_csv(p, encoding="utf-8-sig")
            except pd.errors.EmptyDataError:
                return pd.DataFrame()
            except pd.errors.ParserError as e:
                if "No columns to parse" in str(e):
                    return pd.DataFrame()
                raise RuntimeError(f"결과 파일 읽기 실패 ({p.name}): {e}") from e
            except Exception as e:
                if "No columns to parse" in str(e):
                    return pd.DataFrame()
                raise RuntimeError(f"결과 파일 읽기 실패 ({p.name}): {e}") from e

        try:
            daily = safe_read_csv(output_dir / "daily_summary.csv")
            payroll = safe_read_csv(output_dir / "payroll_result.csv")
            anomaly = safe_read_csv(output_dir / "anomaly_report.csv")
        except Exception as e:
            flash(f"결과를 불러오지 못했습니다. {e}", "error")
            return _err_template()

        if daily.empty or payroll.empty:
            flash(
                "처리 후 근무 데이터가 없습니다. 엑셀에 사원번호·직원·날짜·출근시간·퇴근시간 컬럼이 있는지 확인해 주세요.",
                "error",
            )
            return _err_template()

        daily["_date_str"] = pd.to_datetime(daily["date"]).astype(str).str[:10]
        date_columns = sorted(daily["_date_str"].unique())
        date_headers = [f"{pd.to_datetime(d).month}/{pd.to_datetime(d).day}" for d in date_columns]
        pay_cols = [
            "base_pay",
            "overtime_pay",
            "overtime_hours",
            "weekly_allowance_pay",
            "weekly_allowance_hours",
            "unpaid_hours",
            "total_pay",
        ]
        pay_col_display = {
            "base_pay": "기본급",
            "overtime_pay": "야근수당",
            "overtime_hours": "야근시간",
            "weekly_allowance_pay": "주휴수당",
            "weekly_allowance_hours": "주휴수당<br>산정시간",
            "unpaid_hours": "무급시간",
            "total_pay": "합산금액",
        }
        payroll.columns = [str(c).strip().lstrip("\ufeff") for c in payroll.columns]
        date_cols_in_payroll = [
            c for c in payroll.columns if c not in ["employee_id", "employee_name", "first_attendance_date"] + pay_cols
        ]
        header_to_date = dict(zip(date_headers, date_columns))

        def _header_to_date_key(col):
            base = col.split("\n")[0].split(" ")[0].strip()
            val = header_to_date.get(base) or header_to_date.get(col)
            return str(val)[:10] if val is not None else None

        payroll_cell_list = []
        for c in date_cols_in_payroll:
            if c.startswith("주휴"):
                payroll_cell_list.append((c, None, "weekly"))
            else:
                payroll_cell_list.append((c, _header_to_date_key(c), "date"))

        day_flags = {}
        for _, r in daily.iterrows():
            dkey = str(r["_date_str"])[:10]
            key = f"{r['employee_id']}|{dkey}"
            day_flags[key] = r.get("day_highlight", "normal")

        non_scheduled_set = set()
        if "contract_type" in daily.columns and CONTRACT_CONFIG_PATH.exists():
            try:
                with open(CONTRACT_CONFIG_PATH, "r", encoding="utf-8") as f:
                    contract_cfg = yaml.safe_load(f) or {}
                types_cfg = contract_cfg.get("contract_types", {})
                emp_contract = daily.groupby("employee_id")["contract_type"].first().to_dict()
                for emp in emp_contract:
                    ctype = emp_contract[emp]
                    defn = types_cfg.get(ctype)
                    if not defn or not defn.get("weekdays"):
                        continue
                    weekdays = set(defn["weekdays"])
                    for d in date_columns:
                        wd = pd.to_datetime(d).weekday()
                        if wd not in weekdays:
                            non_scheduled_set.add(f"{emp}|{d[:10]}")
            except Exception:
                pass
        daily.drop(columns=["_date_str"], inplace=True)

        payroll_rows = payroll.fillna("").to_dict("records")
        emp_sched_min = {}
        if "scheduled_minutes" in daily.columns:
            for eid, m in daily.groupby("employee_id")["scheduled_minutes"].first().items():
                if pd.notna(m):
                    emp_sched_min[str(eid).strip()] = float(m)
        contract_types, employee_contracts = {}, {}
        if CONTRACT_CONFIG_PATH.exists():
            try:
                from attendance_normalizer import load_contract_config, get_contract_for_employee

                contract_types, employee_contracts = load_contract_config()
            except Exception:
                pass
        _ref_date = "2025-01-06"

        def _scheduled_hours(eid):
            key = str(eid).strip() if eid is not None else ""
            if contract_types and employee_contracts:
                try:
                    _, scheduled = get_contract_for_employee(eid, _ref_date, contract_types, employee_contracts)
                    if scheduled and scheduled > 0:
                        return round(float(scheduled) / 60, 1)
                except Exception:
                    pass
            m = emp_sched_min.get(key, 480)
            return round(float(m) / 60, 1)

        contract_options = []
        if contract_types:
            for cid, defn in contract_types.items():
                sm = int(defn.get("scheduled_minutes", 480))
                contract_options.append(
                    {
                        "id": cid,
                        "label": f"{cid} ({sm // 60}h)",
                        "scheduled_hours": round(sm / 60, 1),
                    }
                )
            contract_options.sort(key=lambda x: (-x["scheduled_hours"], x["id"]))

        from google_sheet_exporter import REGULAR_EMPLOYEE_IDS

        def _emp_type(eid):
            s = str(eid or "").strip()
            try:
                f = float(s)
                s = str(int(f)) if f == int(f) else s
            except (ValueError, TypeError):
                pass
            if s.upper().startswith("F"):
                return "프리랜스"
            if s in REGULAR_EMPLOYEE_IDS:
                return "정규직"
            return "상용직"

        for row in payroll_rows:
            eid = row.get("employee_id")
            row["_emp_type"] = _emp_type(eid)
            row["_scheduled_hours"] = _scheduled_hours(eid)
            row["_no_id_contract"] = isinstance(eid, str) and eid.strip().startswith("미지정")
            over_8_cols = set()
            sched_hrs = row.get("_scheduled_hours")
            for c in date_cols_in_payroll:
                v = row.get(c)
                if v is None or v == "":
                    continue
                try:
                    s = str(v).strip().replace(",", ".")
                    if not s:
                        continue
                    raw = float(s)
                    if raw > 8.0:
                        over_8_cols.add(c)
                except (TypeError, ValueError):
                    pass
            row["_over_8_cols"] = over_8_cols

        holiday_cols = {}
        freelancer_ids = {}
        if len(date_columns) >= 1:
            try:
                from leave_merger import get_weekday_public_holidays_kr

                d_min = pd.to_datetime(date_columns[0]).date()
                d_max = pd.to_datetime(date_columns[-1]).date()
                holiday_dates_set = get_weekday_public_holidays_kr(d_min, d_max)
                holiday_date_keys = {d.strftime("%Y-%m-%d") for d in holiday_dates_set}
                for c, date_key, typ in payroll_cell_list:
                    if typ == "date" and date_key and (date_key[:10] in holiday_date_keys):
                        col_header = (c.split("\n")[0].split(" ")[0].strip() if c else "")
                        if col_header:
                            holiday_cols[col_header] = True
                for row in payroll_rows:
                    if row.get("_emp_type") == "프리랜스":
                        eid = row.get("employee_id")
                        if eid is not None and (not isinstance(eid, float) or not pd.isna(eid)):
                            freelancer_ids[str(eid).strip()] = True
                            try:
                                if isinstance(eid, (int, float)) and str(eid) != "nan":
                                    freelancer_ids[str(int(eid))] = True
                            except (ValueError, TypeError):
                                pass
            except Exception:
                pass

        hour_cols = {"overtime_hours", "weekly_allowance_hours", "unpaid_hours"}
        for row in payroll_rows:
            for c in pay_cols:
                val = row.get(c)
                if val == "" or val is None:
                    continue
                try:
                    if c in hour_cols:
                        row[c] = f"{float(val):.1f}"
                    else:
                        row[c] = f"{float(val):,.0f}"
                except (TypeError, ValueError):
                    pass
        use_payroll_table = len(date_cols_in_payroll) > 0
        payroll_html_fallback = None if use_payroll_table else payroll.to_html(classes="table", index=False)

        try:
            daily_html = daily.to_html(classes="table", index=False)
            anomaly_html = anomaly.to_html(classes="table", index=False) if not anomaly.empty else None
        except Exception as e:
            flash(f"결과 테이블 생성 중 오류: {e}", "error")
            return _err_template()

        can_payroll_edit = _can_current_user("payroll", "edit")
        can_edit = can_payroll_edit and ((not read_only) or allow_published_edit)
        save_published_url = (
            url_for("save_published_payroll") if allow_published_edit and can_payroll_edit else None
        )
        html = render_template(
            "result.html",
            export_url=url_for("export_google_sheet"),
            save_published_url=save_published_url,
            daily=daily_html,
            use_payroll_table=use_payroll_table,
            payroll_rows=payroll_rows,
            payroll_html_fallback=payroll_html_fallback,
            date_cols_in_payroll=date_cols_in_payroll,
            payroll_cell_list=payroll_cell_list,
            pay_cols=pay_cols,
            pay_col_display=pay_col_display,
            day_flags=day_flags,
            non_scheduled_set=non_scheduled_set,
            anomaly=anomaly_html,
            employee_ids=daily["employee_id"].unique().tolist(),
            contract_options=contract_options,
            holiday_cols=holiday_cols,
            freelancer_ids=freelancer_ids,
            result_read_only=read_only,
            result_back_href=back_href,
            result_back_label=back_label,
            export_allowed=can_payroll_edit,
            table_editable=can_edit,
        )
        resp = make_response(html)
        resp.headers["Content-Type"] = "text/html; charset=utf-8"
        return resp
    except Exception as e:
        flash(f"처리 중 오류: {e}", "error")
        return _err_template()


@app.context_processor
def inject_nav():
    return {
        "is_admin_user": is_current_user_admin(),
        "can_view_payroll": _can_current_user("payroll", "view"),
        "can_view_dashboard": _can_current_user("dashboard", "view"),
        "can_view_commercialization": _can_current_user("commercialization", "view"),
        "can_view_overtime": _can_current_user("overtime", "view"),
        "can_view_admin_data": _can_current_user("admin_data", "view"),
        "can_edit_admin_data": _can_current_user("admin_data", "edit"),
        "can_manage_permissions": is_current_user_admin(),
        "app_version": app_version_display(),
        "fm_roster_on_disk": FM_ROSTER_LOCAL_PATH.is_file(),
    }


@app.route("/", methods=["GET"])
def index():
    if not _published_exists():
        return render_template("public_home.html")
    if _can_current_user("payroll", "view"):
        return redirect(url_for("payroll"))
    if _can_current_user("dashboard", "view"):
        return redirect(url_for("dashboard"))
    if _can_current_user("overtime", "view"):
        return redirect(url_for("overtime_status"))
    if _can_current_user("commercialization", "view"):
        return redirect(url_for("commercialization_dashboard"))
    if _can_current_user("admin_data", "view"):
        return redirect(url_for("admin_data"))
    return render_template("public_home.html")


@app.route("/payroll", methods=["GET"])
def payroll():
    """공개 payroll_result 테이블(모든 로그인 사용자)."""
    if not _can_current_user("payroll", "view"):
        flash("급여 데이터 조회 권한이 없습니다.", "error")
        return redirect(url_for("index"))
    if not _published_exists():
        return redirect(url_for("index"))
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        if not _download_published_to_dir(tmp_dir):
            return redirect(url_for("index"))
        return _make_payroll_result_response(tmp_dir, read_only=True, allow_published_edit=True)


@app.route("/payroll/save", methods=["POST"])
def save_published_payroll():
    """관리자가 급여 테이블을 수정한 뒤 공개본(published)으로 저장 — GCS·로컬 output/published 갱신."""
    if not _can_current_user("payroll", "edit"):
        return jsonify({"ok": False, "error": "급여 데이터 저장 권한이 없습니다."}), 403
    data = request.get_json(silent=True) or {}
    rows = data.get("rows")
    if not rows:
        return jsonify({"ok": False, "error": "저장할 테이블 데이터(rows)가 없습니다."}), 400
    if not _published_exists():
        return jsonify({"ok": False, "error": "공개 급여 데이터가 없습니다."}), 400

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        if not _download_published_to_dir(tmp_dir):
            return jsonify({"ok": False, "error": "기존 공개 데이터를 불러오지 못했습니다."}), 500
        ok, err = _apply_browser_rows_to_payroll_csv(tmp_dir, rows)
        if not ok:
            return jsonify({"ok": False, "error": err or "저장 실패"}), 400

        # Railway: 컨테이너 디스크만 쓰면 재배포 시 사라지므로 GCS 동기화를 기본 필수로 한다.
        if _is_railway_deploy() and not _publish_allow_local_only():
            if not _gcs_env_configured():
                return jsonify(
                    {
                        "ok": False,
                        "error": "Railway에서는 재배포 후에도 유지되도록 GCS에 올려야 합니다. "
                        "GCP_PROJECT_ID, GCS_BUCKET, GOOGLE_APPLICATION_CREDENTIALS_JSON을 설정하세요. "
                        "(임시로 로컬만: ALLOW_PUBLISH_WITHOUT_GCS=1)",
                    }
                ), 500
            if not gcs_enabled():
                return jsonify(
                    {
                        "ok": False,
                        "error": "GCS 설정은 있으나 google-cloud-storage를 불러올 수 없습니다. 빌드에 requirements를 확인하세요.",
                    }
                ), 500

        PUBLISHED_DIR.mkdir(parents=True, exist_ok=True)
        shutil.copy2(tmp_dir / "payroll_result.csv", PUBLISHED_DIR / "payroll_result.csv")

        gcs_ok = False
        if gcs_enabled():
            try:
                _gcs_upload_file(
                    tmp_dir / "payroll_result.csv",
                    _published_blob_name("payroll_result.csv"),
                    content_type="text/csv; charset=utf-8",
                )
                meta = {
                    "published_at": datetime.now(KST).isoformat(),
                    "source": "payroll_table_save",
                    "saved_by": (session.get("user_email") or ""),
                }
                _gcs_upload_text(
                    json.dumps(meta, ensure_ascii=False, indent=2),
                    _published_blob_name("meta.json"),
                )
                gcs_ok = True
            except Exception as e:
                return jsonify({"ok": False, "error": f"GCS 업로드 실패: {e}"}), 500
        elif _gcs_env_configured() and not gcs_enabled():
            return jsonify(
                {
                    "ok": False,
                    "error": "GCS 라이브러리를 불러오지 못해 원격 저장에 실패했습니다. 환경을 확인하세요.",
                }
            ), 500

        try:
            _rebuild_dashboard_cache_from_dir(tmp_dir, source="payroll_table_save")
        except Exception:
            logger.exception("dashboard cache rebuild after payroll/save")

        msg = "공개 급여 데이터가 저장되었습니다."
        if gcs_ok:
            msg += " (GCS published/ 동기화됨)"
        elif not _is_railway_deploy():
            msg += " (서버 로컬 output/published만; GCS 미설정 시 재배포 시 유실 가능)"
        return jsonify({"ok": True, "message": msg, "gcs_synced": gcs_ok})


@app.route("/overtime-status/save", methods=["POST"])
def save_overtime_status():
    """관리자 전용: 연장근무 현황 테이블 최신본 저장(로컬 metadata + GCS metadata)."""
    import pandas as pd

    if not _can_current_user("overtime", "edit"):
        return jsonify({"ok": False, "error": "연장근무 현황 저장 권한이 없습니다."}), 403

    data = request.get_json(silent=True) or {}
    rows_in = data.get("rows")
    work_month = _normalize_work_month(data.get("work_month"), datetime.now(KST).strftime("%Y-%m"))
    if not isinstance(rows_in, list) or not rows_in:
        return jsonify({"ok": False, "error": "저장할 테이블 데이터(rows)가 없습니다."}), 400

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        _attach_overtime_status_to_dir(tmp_dir)
        if not _attach_fm_roster_to_dir(tmp_dir):
            return jsonify({"ok": False, "error": "FM 기본정보가 없어 저장할 수 없습니다."}), 400

        roster_rows = _load_fm_role_name_rows(tmp_dir / FM_ROSTER_FILENAME)
        if not roster_rows:
            return jsonify({"ok": False, "error": "FM 기본정보(역할/닉네임(이름))를 확인해 주세요."}), 400
        date_keys = _work_month_date_keys(work_month)
        if not date_keys:
            return jsonify({"ok": False, "error": "급여 데이터에서 일자를 만들 수 없습니다."}), 400

        allowed_row_keys = {f"{r['role']}|{r['display_name']}" for r in roster_rows}
        incoming_map: dict[str, dict] = {}
        for r in rows_in:
            role = str((r or {}).get("role", "")).strip()
            display_name = str((r or {}).get("display_name", "")).strip()
            if not role or not display_name:
                continue
            row_key = f"{role}|{display_name}"
            if row_key not in allowed_row_keys:
                continue
            row_clean = {}
            for d in date_keys:
                raw = (r or {}).get(d, "")
                if raw is None:
                    row_clean[d] = ""
                    continue
                s = str(raw).strip().replace(",", "")
                if not s:
                    row_clean[d] = ""
                    continue
                try:
                    val = float(s)
                except (TypeError, ValueError):
                    return jsonify({"ok": False, "error": f"숫자만 입력할 수 있습니다. ({display_name}, {d})"}), 400
                row_clean[d] = f"{val:g}"
            incoming_map[row_key] = row_clean

        out_rows = []
        for r in roster_rows:
            row_key = f"{r['role']}|{r['display_name']}"
            vals = incoming_map.get(row_key, {})
            out = {"role": r["role"], "display_name": r["display_name"]}
            for d in date_keys:
                out[d] = vals.get(d, "")
            out_rows.append(out)

        # long 포맷으로 월별 누적 저장: work_month, role, display_name, date, value
        existing_long = _safe_read_csv(tmp_dir / OVERTIME_STATUS_FILENAME)
        if not existing_long.empty:
            existing_long.columns = [str(c).strip().lstrip("\ufeff") for c in existing_long.columns]
            need_cols = {"work_month", "role", "display_name", "date", "value"}
            if not need_cols.issubset(set(existing_long.columns)):
                # 구버전 wide 포맷은 migration하지 않고 이번 저장월부터 long 누적 시작
                existing_long = pd.DataFrame(columns=["work_month", "role", "display_name", "date", "value"])
            else:
                existing_long = existing_long[["work_month", "role", "display_name", "date", "value"]].copy()
        else:
            existing_long = pd.DataFrame(columns=["work_month", "role", "display_name", "date", "value"])

        if not existing_long.empty:
            wm = existing_long["work_month"].astype(str).str.strip()
            existing_long = existing_long[wm != work_month].copy()

        new_long_rows = []
        for row in out_rows:
            role = row.get("role", "")
            display_name = row.get("display_name", "")
            for d in date_keys:
                v = str(row.get(d, "")).strip()
                if not v:
                    continue
                new_long_rows.append(
                    {
                        "work_month": work_month,
                        "role": role,
                        "display_name": display_name,
                        "date": d,
                        "value": v,
                    }
                )
        new_long_df = pd.DataFrame(new_long_rows, columns=["work_month", "role", "display_name", "date", "value"])
        save_df = pd.concat([existing_long, new_long_df], ignore_index=True)
        if not save_df.empty:
            save_df = save_df.drop_duplicates(subset=["work_month", "role", "display_name", "date"], keep="last")
            save_df.sort_values(["work_month", "role", "display_name", "date"], inplace=True)

        save_csv_path = tmp_dir / OVERTIME_STATUS_FILENAME
        save_df.to_csv(save_csv_path, index=False, encoding="utf-8-sig")

        if _is_railway_deploy() and not _publish_allow_local_only():
            if not _gcs_env_configured():
                return jsonify(
                    {
                        "ok": False,
                        "error": "Railway에서는 재배포 후에도 유지되도록 GCS에 올려야 합니다. "
                        "GCP_PROJECT_ID, GCS_BUCKET, GOOGLE_APPLICATION_CREDENTIALS_JSON을 설정하세요. "
                        "(임시로 로컬만: ALLOW_PUBLISH_WITHOUT_GCS=1)",
                    }
                ), 500
            if not gcs_enabled():
                return jsonify(
                    {
                        "ok": False,
                        "error": "GCS 설정은 있으나 google-cloud-storage를 불러올 수 없습니다. 빌드에 requirements를 확인하세요.",
                    }
                ), 500

        FM_ROSTER_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
        shutil.copy2(save_csv_path, OVERTIME_STATUS_LOCAL_PATH)
        meta = {
            "saved_at": datetime.now(KST).isoformat(),
            "saved_by": (session.get("user_email") or ""),
            "work_month": work_month,
            "row_count": len(out_rows),
            "date_count": len(date_keys),
            "non_empty_cell_count": len(new_long_rows),
        }
        OVERTIME_STATUS_META_LOCAL_PATH.write_text(
            json.dumps(meta, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        _view_cache_clear("overtime:")

        gcs_ok = False
        if gcs_enabled():
            try:
                _gcs_upload_file(
                    OVERTIME_STATUS_LOCAL_PATH,
                    _overtime_status_blob_name(),
                    content_type="text/csv; charset=utf-8",
                )
                _gcs_upload_text(
                    json.dumps(meta, ensure_ascii=False, indent=2),
                    _overtime_status_meta_blob_name(),
                )
                gcs_ok = True
            except Exception as e:
                return jsonify({"ok": False, "error": f"GCS 업로드 실패: {e}"}), 500
        elif _gcs_env_configured() and not gcs_enabled():
            return jsonify(
                {
                    "ok": False,
                    "error": "GCS 라이브러리를 불러오지 못해 원격 저장에 실패했습니다. 환경을 확인하세요.",
                }
            ), 500

        msg = f"연장근무 현황이 저장되었습니다. (근로월 {work_month})"
        if gcs_ok:
            msg += " (GCS metadata/ 동기화됨)"
        elif not _is_railway_deploy():
            msg += " (서버 로컬 output/metadata만; GCS 미설정 시 재배포 시 유실 가능)"
        return jsonify({"ok": True, "message": msg, "gcs_synced": gcs_ok})


@app.route("/admin/permissions", methods=["POST"])
def admin_permissions():
    # 권한 편집 권한은 super-admin(ADMIN_EMAILS)만 허용: 임의 권한 승격 방지
    if not is_current_user_admin():
        flash("권한 관리는 최고 관리자만 변경할 수 있습니다.", "error")
        return redirect(url_for("admin_data"))
    payload_raw = (request.form.get("permissions_payload") or "").strip()
    if not payload_raw:
        flash("저장할 권한 데이터가 없습니다.", "error")
        return redirect(url_for("admin_data"))
    try:
        parsed = json.loads(payload_raw)
    except json.JSONDecodeError:
        flash("권한 데이터 형식이 올바르지 않습니다.", "error")
        return redirect(url_for("admin_data"))
    if not isinstance(parsed, list):
        flash("권한 데이터 형식이 올바르지 않습니다.", "error")
        return redirect(url_for("admin_data"))
    rows = _clean_permission_rows(parsed)
    ok, err = _save_permissions_config(rows, updated_by=(session.get("user_email") or ""))
    if not ok:
        flash(err or "권한 저장 실패", "error")
        return redirect(url_for("admin_data"))
    flash(f"권한 설정이 저장되었습니다. ({len(rows)}개 계정)", "success")
    return redirect(url_for("admin_data"))


@app.route("/admin/fm-roster", methods=["POST"])
def admin_fm_roster():
    """FM 기본정보 xlsx — 로컬 output/metadata + GCS metadata/fm_roster.xlsx."""
    if not _can_current_user("admin_data", "edit"):
        flash("관리자 데이터 수정 권한이 없습니다.", "error")
        return redirect(url_for("index"))
    f = request.files.get("file_fm")
    if not f or not f.filename or not str(f.filename).strip():
        flash("FM 목록 파일을 선택해 주세요.", "error")
        return redirect(url_for("admin_data"))
    if not f.filename.lower().endswith((".xlsx", ".xls")):
        flash("FM 목록은 .xlsx 또는 .xls 만 업로드할 수 있습니다.", "error")
        return redirect(url_for("admin_data"))
    try:
        FM_ROSTER_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
        dest = FM_ROSTER_LOCAL_PATH
        f.save(str(dest))
        fm_data = _load_fm_roster_data(dest)
        if fm_data is None:
            dest.unlink(missing_ok=True)
            flash("FM 엑셀에 필수 컬럼(사번, 역할)이 없거나 내용을 읽을 수 없습니다.", "error")
            return redirect(url_for("admin_data"))
        pairs, name_map = fm_data
        n = int(len(pairs)) + int(len(name_map))
        if gcs_enabled():
            ct = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            if dest.suffix.lower() == ".xls":
                ct = "application/vnd.ms-excel"
            _gcs_upload_file(dest, _fm_roster_blob_name(), content_type=ct)
        elif _gcs_env_configured() and not gcs_enabled():
            flash(
                "GCS 라이브러리를 불러오지 못해 원격에는 저장하지 못했습니다. 로컬에만 저장되었습니다.",
                "warning",
            )
        fm_meta = {
            "filename": f.filename,
            "uploaded_at": datetime.now(KST).isoformat(),
            "uploaded_by": (session.get("user_email") or ""),
        }
        FM_UPLOAD_META_LOCAL_PATH.write_text(
            json.dumps(fm_meta, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        if gcs_enabled():
            _gcs_upload_text(
                json.dumps(fm_meta, ensure_ascii=False, indent=2),
                _fm_upload_meta_blob_name(),
            )
        _view_cache_clear("dashboard")
        _view_cache_clear("overtime:")
        if _published_exists():
            with tempfile.TemporaryDirectory() as t2:
                pdir = Path(t2)
                if _download_published_to_dir(pdir):
                    try:
                        _rebuild_dashboard_cache_from_dir(pdir, source="fm_roster_upload")
                    except Exception:
                        logger.exception("dashboard cache rebuild after fm roster upload")
        flash(
            f"FM 기본정보를 반영했습니다. (사번 {len(pairs)}건 + 이름 보조 {len(name_map)}건 = 매칭 키 {n}건, 대시보드 역할별 집계에 사용)",
            "success",
        )
    except Exception as e:
        logger.exception("FM roster upload")
        flash(f"FM 파일 저장 실패: {e}", "error")
    return redirect(url_for("admin_data"))


@app.route("/admin/data", methods=["GET", "POST"])
@app.route("/admin", methods=["GET", "POST"])
def admin_data():
    if not _can_current_user("admin_data", "view"):
        flash("관리자 데이터 조회 권한이 없습니다.", "error")
        return redirect(url_for("index"))
    if request.method == "GET":
        return render_template("upload.html", **_admin_upload_display_context())
    if not _can_current_user("admin_data", "edit"):
        flash("관리자 데이터 수정 권한이 없습니다.", "error")
        return render_template("upload.html", **_admin_upload_display_context())

    file = request.files.get("file")
    if not file or file.filename == "":
        flash("엑셀 파일을 선택해 주세요.", "error")
        return render_template("upload.html", **_admin_upload_display_context())

    if not file.filename.lower().endswith((".xlsx", ".xls")):
        flash("엑셀 파일(.xlsx, .xls)만 업로드 가능합니다.", "error")
        return render_template("upload.html", **_admin_upload_display_context())

    try:
        import pandas as pd

        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            input_path = tmp / "upload.xlsx"
            file.save(str(input_path))

            if input_path.stat().st_size == 0:
                flash("업로드한 파일이 비어 있습니다. 시프티 출퇴근 엑셀(.xlsx)을 다시 내보내 주세요.", "error")
                return render_template("upload.html", **_admin_upload_display_context())

            try:
                trial = pd.read_excel(input_path)
                if trial.empty or len(trial) == 0:
                    flash("엑셀에 데이터 행이 없습니다. 시프티에서 올바른 기간으로 출퇴근 내역을 내보내 주세요.", "error")
                    return render_template("upload.html", **_admin_upload_display_context())
                required = ["사원번호", "직원", "날짜", "출근시간", "퇴근시간"]
                missing = [c for c in required if c not in trial.columns]
                if missing:
                    flash(
                        f"엑셀에 필수 컬럼이 없습니다: {', '.join(missing)}. 시프티 출퇴근 형식인지 확인해 주세요. (현재 컬럼: {list(trial.columns)[:10]}…)",
                        "error",
                    )
                    return render_template("upload.html", **_admin_upload_display_context())
            except Exception as e:
                flash(f"엑셀 파일을 열 수 없습니다. ({e})", "error")
                return render_template("upload.html", **_admin_upload_display_context())

            run_dir = tmp / "published"
            if run_dir.exists():
                shutil.rmtree(run_dir)
            run_dir.mkdir(parents=True, exist_ok=True)

            file_leave = request.files.get("file_leave")
            leave_path, leave_source = _resolve_leave_path_for_upload(tmp, file_leave)
            if leave_source == "invalid":
                flash("휴가 파일은 엑셀(.xlsx, .xls)만 업로드 가능합니다.", "error")
                return render_template("upload.html", **_admin_upload_display_context())

            try:
                from run_all import run_pipeline

                run_pipeline(input_path=input_path, output_dir=run_dir, leave_path=leave_path)
            except Exception as e:
                flash(f"파이프라인 처리 중 오류: {e}", "error")
                return render_template("upload.html", **_admin_upload_display_context())

            if PUBLISHED_DIR.exists():
                shutil.rmtree(PUBLISHED_DIR)
            shutil.copytree(run_dir, PUBLISHED_DIR)
            _sync_run_to_gcs(
                run_dir,
                input_path=input_path,
                leave_path=leave_path,
                uploaded_by=(session.get("user_email") or ""),
            )
            try:
                _rebuild_dashboard_cache_from_dir(run_dir, source="admin_upload")
            except Exception:
                logger.exception("dashboard cache rebuild after admin upload")
            if _gcs_env_configured() and not gcs_enabled():
                flash(
                    "GCS 환경변수는 있으나 google-cloud-storage 라이브러리를 불러오지 못했습니다. "
                    "Railway 빌드 로그에서 해당 패키지 설치 여부를 확인한 뒤 재배포해 주세요.",
                    "warning",
                )
            session["last_run_id"] = PUBLISHED_ID
            if leave_source in ("local_cached", "gcs_cached", "gcs_legacy"):
                flash("휴가 파일을 새로 첨부하지 않아, 마지막 휴가 데이터를 자동 반영해 갱신했습니다.", "success")
            else:
                flash("공개 급여 데이터가 갱신되었습니다.", "success")
            return redirect(url_for("payroll"))
    except Exception as e:
        flash(f"처리 중 오류: {e}", "error")
        return render_template("upload.html", **_admin_upload_display_context())


@app.route("/export-google-sheet", methods=["GET", "POST"])
def export_google_sheet():
    """payroll_result를 구글 시트로 내보내기. 공개(published) 또는 세션의 마지막 run."""
    if request.method == "GET":
        return jsonify({"ok": True, "message": "POST로 요청하세요."})

    if not _can_current_user("payroll", "edit"):
        return jsonify({"ok": False, "error": "구글 시트 내보내기 권한이 없습니다."}), 403

    resolved = resolve_export_output_dir()
    if not resolved:
        return jsonify({"ok": False, "error": "내보낼 결과가 없습니다. 관리자가 먼저 급여 데이터를 등록해 주세요."}), 400
    with tempfile.TemporaryDirectory() as tmp:
        output_dir = Path(tmp)
        if resolved == PUBLISHED_DIR and gcs_enabled():
            if not _download_published_to_dir(output_dir):
                return jsonify({"ok": False, "error": "공개 결과를 GCS에서 불러오지 못했습니다."}), 404
        else:
            for name in PUBLISHED_FILES:
                src = resolved / name
                if src.exists():
                    shutil.copy2(src, output_dir / name)

        # 브라우저에서 수정한 급여 테이블이 JSON(rows)로 넘어온 경우,
        # 기존 payroll_result.csv를 이 데이터로 덮어쓴 뒤 그 파일을 기준으로 구글 시트를 생성한다.
        data = request.get_json(silent=True) or {}
        rows = data.get("rows")
        if rows:
            ok, err = _apply_browser_rows_to_payroll_csv(output_dir, rows)
            if not ok:
                return jsonify({"ok": False, "error": err or "수정된 테이블을 반영하지 못했습니다."}), 400

        try:
            from google_sheet_exporter import create_google_sheet

            url = create_google_sheet(output_dir)
            return jsonify({"ok": True, "url": url})
        except FileNotFoundError as e:
            return jsonify({"ok": False, "error": str(e)}), 404
        except RuntimeError as e:
            return jsonify({"ok": False, "error": str(e)}), 500
        except Exception as e:
            logger.exception("export_google_sheet")
            detail = str(e).strip()
            if not detail:
                detail = repr(e)
            if not detail or detail == "()":
                detail = type(e).__name__
            return jsonify({"ok": False, "error": f"내보내기 실패: {detail}"}), 500


if __name__ == "__main__":
    # use_reloader=False: Python 표준 라이브러리 변경까지 감지해 재시작되는 것 방지
    app.run(debug=True, port=5000, use_reloader=False)
