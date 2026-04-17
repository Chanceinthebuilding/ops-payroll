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
import yaml
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from flask import Flask, make_response, redirect, render_template, request, flash, session, jsonify, url_for
from werkzeug.middleware.proxy_fix import ProxyFix

from auth_google import auth_disabled, register_google_auth

ROOT = Path(__file__).resolve().parent
OUTPUT_BASE = ROOT / "output"
PUBLISHED_ID = "published"
PUBLISHED_DIR = OUTPUT_BASE / PUBLISHED_ID
PUBLISHED_FILES = ("daily_summary.csv", "payroll_result.csv", "anomaly_report.csv")
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
    if not _published_exists():
        return render_template("dashboard.html", dashboard_ready=False)
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        if not _download_published_to_dir(tmp_dir):
            return render_template("dashboard.html", dashboard_ready=False)
        ctx = _build_dashboard_context(tmp_dir)
        return render_template("dashboard.html", **ctx)


@app.route("/healthz")
def healthz():
    return jsonify({"ok": True}), 200


def _gcs_bucket_name() -> str:
    return os.environ.get("GCS_BUCKET", "").strip()


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


def _sync_run_to_gcs(run_dir: Path, input_path: Path, leave_path: Path | None = None, uploaded_by: str | None = None):
    if not gcs_enabled():
        return
    stamp = datetime.now(KST).strftime("%Y-%m-%d_%H%M%S")
    run_prefix = f"runs/{stamp}"
    _gcs_upload_file(input_path, f"uploads/attendance/{stamp}{input_path.suffix}")
    if leave_path and leave_path.exists():
        _gcs_upload_file(leave_path, f"uploads/leave/{stamp}{leave_path.suffix}")
    for name in PUBLISHED_FILES:
        src = run_dir / name
        if not src.exists():
            continue
        _gcs_upload_file(src, f"{run_prefix}/{name}", content_type="text/csv; charset=utf-8")
        _gcs_upload_file(src, _published_blob_name(name), content_type="text/csv; charset=utf-8")
    meta = {
        "published_at": datetime.now(KST).isoformat(),
        "uploaded_by": uploaded_by or "",
        "run_prefix": run_prefix,
        "attendance_name": input_path.name,
        "leave_name": leave_path.name if leave_path else "",
    }
    _gcs_upload_text(json.dumps(meta, ensure_ascii=False, indent=2), _published_blob_name("meta.json"))


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
    chart_labels = daily_cost["date_key"].tolist()
    chart_values = [round(float(v), 0) for v in daily_cost["daily_cost"].tolist()]

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
    first_date = daily_cost["date_key"].iloc[0] if len(daily_cost) else "-"
    last_date = daily_cost["date_key"].iloc[-1] if len(daily_cost) else "-"

    return {
        "dashboard_ready": True,
        "kpi_total_pay": total_pay,
        "kpi_total_employees": total_employees,
        "kpi_total_work_hours": total_work_hours,
        "kpi_anomaly_count": anomaly_count,
        "period_start": first_date,
        "period_end": last_date,
        "chart_labels": chart_labels,
        "chart_values": chart_values,
        "weekly_chart_labels": weekly_chart_labels,
        "weekly_chart_values": weekly_chart_values,
        "top_employee_rows": top_employee_rows,
        "daily_rank_rows": daily_rank_rows,
    }


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


def _make_payroll_result_response(
    output_dir: Path,
    *,
    read_only: bool = False,
    back_href: str | None = None,
    back_label: str | None = None,
):
    import pandas as pd

    if back_href is None:
        back_href = url_for("index")
    if back_label is None:
        back_label = "← 홈" if read_only else "← 급여 데이터 관리"

    def _err_template():
        return render_template("public_home.html") if read_only else render_template("upload.html")

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

        can_edit = (not read_only) and is_current_user_admin()
        html = render_template(
            "result.html",
            export_url=url_for("export_google_sheet"),
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
            export_allowed=is_current_user_admin(),
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
    return {"is_admin_user": is_current_user_admin()}


@app.route("/", methods=["GET"])
def index():
    if not _published_exists():
        return render_template("public_home.html")
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        if not _download_published_to_dir(tmp_dir):
            return render_template("public_home.html")
        return _make_payroll_result_response(tmp_dir, read_only=True)


@app.route("/admin", methods=["GET", "POST"])
def admin():
    if not is_current_user_admin():
        flash("관리자만 접근할 수 있습니다.", "error")
        return redirect(url_for("index"))
    if request.method == "GET":
        return render_template("upload.html")

    file = request.files.get("file")
    if not file or file.filename == "":
        flash("엑셀 파일을 선택해 주세요.", "error")
        return render_template("upload.html")

    if not file.filename.lower().endswith((".xlsx", ".xls")):
        flash("엑셀 파일(.xlsx, .xls)만 업로드 가능합니다.", "error")
        return render_template("upload.html")

    try:
        import pandas as pd

        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            input_path = tmp / "upload.xlsx"
            file.save(str(input_path))

            if input_path.stat().st_size == 0:
                flash("업로드한 파일이 비어 있습니다. 시프티 출퇴근 엑셀(.xlsx)을 다시 내보내 주세요.", "error")
                return render_template("upload.html")

            try:
                trial = pd.read_excel(input_path)
                if trial.empty or len(trial) == 0:
                    flash("엑셀에 데이터 행이 없습니다. 시프티에서 올바른 기간으로 출퇴근 내역을 내보내 주세요.", "error")
                    return render_template("upload.html")
                required = ["사원번호", "직원", "날짜", "출근시간", "퇴근시간"]
                missing = [c for c in required if c not in trial.columns]
                if missing:
                    flash(
                        f"엑셀에 필수 컬럼이 없습니다: {', '.join(missing)}. 시프티 출퇴근 형식인지 확인해 주세요. (현재 컬럼: {list(trial.columns)[:10]}…)",
                        "error",
                    )
                    return render_template("upload.html")
            except Exception as e:
                flash(f"엑셀 파일을 열 수 없습니다. ({e})", "error")
                return render_template("upload.html")

            run_dir = tmp / "published"
            if run_dir.exists():
                shutil.rmtree(run_dir)
            run_dir.mkdir(parents=True, exist_ok=True)

            leave_path = None
            file_leave = request.files.get("file_leave")
            if file_leave and file_leave.filename and file_leave.filename.strip():
                if file_leave.filename.lower().endswith((".xlsx", ".xls")):
                    leave_path = tmp / "leave.xlsx"
                    file_leave.save(str(leave_path))

            try:
                from run_all import run_pipeline

                run_pipeline(input_path=input_path, output_dir=run_dir, leave_path=leave_path)
            except Exception as e:
                flash(f"파이프라인 처리 중 오류: {e}", "error")
                return render_template("upload.html")

            if PUBLISHED_DIR.exists():
                shutil.rmtree(PUBLISHED_DIR)
            shutil.copytree(run_dir, PUBLISHED_DIR)
            _sync_run_to_gcs(
                run_dir,
                input_path=input_path,
                leave_path=leave_path,
                uploaded_by=(session.get("user_email") or ""),
            )
            if _gcs_env_configured() and not gcs_enabled():
                flash(
                    "GCS 환경변수는 있으나 google-cloud-storage 라이브러리를 불러오지 못했습니다. "
                    "Railway 빌드 로그에서 해당 패키지 설치 여부를 확인한 뒤 재배포해 주세요.",
                    "warning",
                )
            session["last_run_id"] = PUBLISHED_ID
            flash("공개 급여 데이터가 갱신되었습니다.", "success")
            return redirect(url_for("index"))
    except Exception as e:
        flash(f"처리 중 오류: {e}", "error")
        return render_template("upload.html")


@app.route("/export-google-sheet", methods=["GET", "POST"])
def export_google_sheet():
    """payroll_result를 구글 시트로 내보내기. 공개(published) 또는 세션의 마지막 run."""
    if request.method == "GET":
        return jsonify({"ok": True, "message": "POST로 요청하세요."})

    if not is_current_user_admin():
        return jsonify({"ok": False, "error": "관리자만 구글 시트로 내보낼 수 있습니다."}), 403

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
            try:
                import pandas as pd

                df = pd.DataFrame(rows)
                if not {"employee_id", "employee_name"}.issubset(df.columns):
                    return jsonify({"ok": False, "error": "테이블 데이터에 employee_id/employee_name 컬럼이 없습니다."}), 400

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
                            pay_cols_set = {"base_pay", "overtime_pay", "overtime_hours", "weekly_allowance_pay", "weekly_allowance_hours", "unpaid_hours", "total_pay"}
                            for c in df.columns:
                                if c in ("employee_id", "employee_name", "_contract_override") or c in pay_cols_set or str(c).strip().startswith("주휴") or "주휴용" in str(c):
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
                _recalc_pay_from_date_columns(df, contract_types=contract_types, employee_contracts=employee_contracts, holiday_dates=holiday_dates, date_col_to_date=date_col_to_date)

                df_export = df.drop(columns=["_contract_override"], errors="ignore")
                csv_path = output_dir / "payroll_result.csv"
                df_export.to_csv(csv_path, index=False, encoding="utf-8-sig")
            except Exception as e:
                return jsonify({"ok": False, "error": f"수정된 테이블 데이터를 저장하지 못했습니다: {e}"}), 400

        try:
            from google_sheet_exporter import create_google_sheet
            url = create_google_sheet(output_dir)
            return jsonify({"ok": True, "url": url})
        except FileNotFoundError as e:
            return jsonify({"ok": False, "error": str(e)}), 404
        except RuntimeError as e:
            return jsonify({"ok": False, "error": str(e)}), 500
        except Exception as e:
            return jsonify({"ok": False, "error": f"내보내기 실패: {e}"}), 500


if __name__ == "__main__":
    # use_reloader=False: Python 표준 라이브러리 변경까지 감지해 재시작되는 것 방지
    app.run(debug=True, port=5000, use_reloader=False)
