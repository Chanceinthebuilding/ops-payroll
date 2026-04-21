"""
payroll_result.csv를 구글 시트 형식으로 변환하여 새 시트를 생성합니다.
필요: gspread, google-auth
설정: GOOGLE_APPLICATION_CREDENTIALS_JSON(서비스 계정 JSON 전체 문자열, Railway 권장) 또는
      credentials/service_account.json 또는 GOOGLE_APPLICATION_CREDENTIALS(파일 경로)

급여산정: N월 급여 = 전월 25일 ~ 당월 24일. 기본급 = (11,000 × scheduled_minutes/60 × 영업일) - 200,000(식대). 계약별 scheduled_minutes는 contract_config.yaml 참조.
"""
from __future__ import annotations

import contextlib
import json
import logging
import os
import tempfile
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
logger = logging.getLogger(__name__)


def _credentials_from_service_account_dict(info: dict, scopes: list) -> "Credentials":
    """
    from_service_account_info 대신 임시 JSON 파일로 로드.
    일부 환경에서 메모리 키 처리 시 PermissionError가 나는 경우를 피한다.
    """
    from google.oauth2.service_account import Credentials

    fd, path = tempfile.mkstemp(suffix=".json", text=True)
    path = str(path)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(info, f)
        return Credentials.from_service_account_file(path, scopes=scopes)
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


@contextlib.contextmanager
def _google_api_safe_env():
    """
    Railway/Docker 등에서 기본 HOME·캐시 경로에 쓰기가 막혀 gspread/google-auth가
    PermissionError를 내는 경우가 있어, 시트 내보내기 구간만 임시 디렉터리로 고정한다.
    잘못된 SSL_CERT_FILE 등은 읽기 실패(PermissionError)를 유발할 수 있어 제거한다.
    """
    tmp = Path(tempfile.gettempdir())
    sandbox = tmp / "chaftee_gspread"
    sandbox.mkdir(parents=True, exist_ok=True)
    cache = sandbox / ".cache"
    config = sandbox / ".config"
    cache.mkdir(parents=True, exist_ok=True)
    config.mkdir(parents=True, exist_ok=True)

    keys = ("HOME", "XDG_CACHE_HOME", "XDG_CONFIG_HOME", "TMPDIR")
    saved = {k: os.environ.get(k) for k in keys}

    ssl_keys = ("SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE")
    ssl_removed: dict[str, str | None] = {}
    for sk in ssl_keys:
        v = os.environ.get(sk)
        if v and (not Path(v).is_file()):
            ssl_removed[sk] = v
            os.environ.pop(sk, None)

    try:
        os.environ["HOME"] = str(sandbox)
        os.environ["XDG_CACHE_HOME"] = str(cache)
        os.environ["XDG_CONFIG_HOME"] = str(config)
        os.environ["TMPDIR"] = str(tmp)
        yield
    finally:
        for k in keys:
            v = saved[k]
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        for sk, v in ssl_removed.items():
            if v is not None:
                os.environ[sk] = v

HOURLY_RATE_BASE = 11_000  # 통상시급 (원)
MEAL_ALLOWANCE = 200_000  # 식대 (원), 기본급에서 차감
UNPAID_HOURLY_RATE = 11_000  # 무급시간당 차감 (원), 무급휴가 = unpaid_hours × -11,000

# 기존 스프레드시트에 새 시트 추가 (통째로 새 스프레드시트 생성 대신)
# ※ ops-robot 서비스 계정에 해당 스프레드시트 편집 권한이 있어야 함
TARGET_SPREADSHEET_ID = "18skwWPXVf6aNMyJb8u7PiExSX_dBcqrNrYciZE0DgWY"

# 정규직 사원코드 (이 인원은 상용직 시트에서 제외, 정규직 시트에만 포함)
REGULAR_EMPLOYEE_IDS = {"45", "47", "52", "63", "101", "104", "117", "124", "128", "135", "139", "153","122","123","125"}

# 육아수당 지급 대상 사원코드 (상용직 시트). 기본급에서 200,000원 차감 후 육아수당 항목으로 표시
CHILDCARE_ALLOWANCE_EMPLOYEE_IDS = set()
CHILDCARE_ALLOWANCE_AMOUNT = 200_000  # 원

# 정규직 시트: 사원별 근로시간/휴게시간 (default와 다른 경우만 지정)
REGULAR_WORK_HOURS_BY_ID = {
    "52": "(월~금) 10:00~18:00",  # 류경희
}

# 지급 컬럼 순서 (사용자 제공 형식)
PAY_COLUMNS = [
    "기본급", "상여", "식대", "자가운전보조금", "연구보조금", "육아수당", "출장여비",
    "연차수당", "무급휴가", "야근수당", "연장근로수당", "주휴수당", "연장근로수당2",
    "고정연장수당", "소급분", "벤처주식매수선택권", "지급액계",
]
DEDUCT_COLUMNS = [
    "국민연금", "건강보험", "고용보험", "장기요양보험료", "소득세", "지방소득세",
    "학자금상환액", "건강보험료 정산", "장기요양보험료 정산", "기지급액",
    "연말정산 소득세", "연말정산 지방소득세", "국민연금보혐료 정산",
    "건강보험료 연말정산", "장기요양보험료 연말정산", "건강보험료 환급금이자",
    "무급휴가사용", "공제액계",
]

# payroll_result → 지급 컬럼 매핑 (기본급 제외, 산정일 기반으로 계산)
PAY_MAPPING = {
    "야근수당": "overtime_pay",
    "주휴수당": "weekly_allowance_pay",
    "지급액계": "total_pay",
}


def _payroll_period(payroll_year: int, payroll_month: int) -> tuple[date, date]:
    """N월 급여 산정기간: 전월 25일 ~ 당월 24일."""
    if payroll_month == 1:
        start = date(payroll_year - 1, 12, 25)
    else:
        start = date(payroll_year, payroll_month - 1, 25)
    end = date(payroll_year, payroll_month, 24)
    return start, end


def _count_business_days(start: date, end: date, weekdays: list[int] | None = None) -> int:
    """영업일 수. weekdays=None이면 월~금(0..4), 지정 시 해당 요일만. 빨간날(공휴일)도 평일이면 유급일로 포함."""
    weekdays = weekdays if weekdays is not None else [0, 1, 2, 3, 4]
    n = 0
    d = start
    while d <= end:
        if d.weekday() in weekdays:
            n += 1
        d += timedelta(days=1)
    return n


def _load_contract_config() -> tuple[dict, dict]:
    """(contract_types, employee_contracts) 반환. contract_config.yaml."""
    try:
        import yaml

        p = ROOT / "contract_config.yaml"
        if not p.exists():
            return (
                {"standard_9to6": {"scheduled_minutes": 480, "weekdays": [0, 1, 2, 3, 4]}},
                {"default": "standard_9to6"},
            )
        with open(p, encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        types = cfg.get("contract_types", {"standard_9to6": {"scheduled_minutes": 480, "weekdays": [0, 1, 2, 3, 4]}})
        employees = cfg.get("employee_contracts", {"default": "standard_9to6"})
        return types, employees
    except Exception:
        return (
            {"standard_9to6": {"scheduled_minutes": 480, "weekdays": [0, 1, 2, 3, 4]}},
            {"default": "standard_9to6"},
        )


def _calc_base_pay_for_employee(
    employee_id,
    payroll_year: int,
    payroll_month: int,
    contract_types: dict,
    employee_contracts: dict,
    first_attendance_date: str | date | None = None,
) -> int:
    """기본급 = (11,000 × scheduled_minutes/60 × 영업일) - 200,000 (식대 제외). 계약별.
    first_attendance_date가 있으면(사번 없는 인원 등) 해당일~기간 종료일 기준 영업일로 산정."""
    emp_key = str(employee_id or "").strip()
    try:
        if isinstance(employee_id, (int, float)) and str(employee_id) != "nan":
            emp_key = str(int(employee_id))
    except (ValueError, TypeError):
        pass
    ctype = (
        employee_contracts.get(emp_key)
        or employee_contracts.get("default")
        or "standard_9to6"
    )
    defn = contract_types.get(ctype, {"scheduled_minutes": 480, "weekdays": [0, 1, 2, 3, 4]})
    scheduled_min = int(defn.get("scheduled_minutes", 480))
    weekdays = defn.get("weekdays", [0, 1, 2, 3, 4])
    start, end = _payroll_period(payroll_year, payroll_month)
    if first_attendance_date:
        try:
            start_override = (
                first_attendance_date
                if isinstance(first_attendance_date, date)
                else date(*[int(x) for x in str(first_attendance_date).strip()[:10].split("-")])
            )
            if start_override > start:
                start = start_override
        except Exception:
            pass
    days = _count_business_days(start, end, weekdays)
    daily_rate = HOURLY_RATE_BASE * (scheduled_min / 60)
    return int(daily_rate * days - MEAL_ALLOWANCE)


def _fmt_num(val) -> str:
    """숫자를 천 단위 콤마 문자열로. 음수 포함. 빈칸/None은 ''."""
    if val is None or val == "" or (isinstance(val, float) and str(val) == "nan"):
        return ""
    try:
        n = int(float(val))
        return f"{n:,}"
    except (TypeError, ValueError):
        return str(val) if val else ""


def _parse_nickname_name(employee_name: str) -> tuple[str, str]:
    """
    employee_name '닉네임(성명)' → (닉네임, 성명).
    괄호 없으면 전체를 성명으로 (닉네임 "").
    '('만 있고 ')'가 없어도 첫 '(' 기준으로 분리 (오타·미완성 입력 대비).
    """
    s = str(employee_name or "").strip()
    if not s:
        return "", ""
    if "(" not in s:
        return "", s
    idx = s.index("(")
    nick = s[:idx].strip()
    after = s[idx + 1 :]
    if ")" in after:
        inner = after[: after.index(")")].strip()
    else:
        inner = after.strip()
    return nick, inner


def build_정규직_sheet_data(정규직_df) -> list[list]:
    """
    정규직 전용 양식: 사원코드, 사원명, 주민등록번호, 비고,
    총 야근수당 대상 시간(1.5배), 총 추가근무수당 대상 시간(1배), 총 무급시간(-1배),
    연차수당 대상 시간, 소급분, 일별 근로시간, 근로일 및 근로시간, 휴게시간, 주휴일
    """
    # 야근수당 1.5배 → 시간 환산: overtime_pay / (11000 * 1.5)
    OT_HOURLY_RATE = 11_000 * 1.5  # 16500

    headers = [
        "사원코드", "사원명", "주민등록번호", "비고",
        "총 야근수당 대상 시간 (1.5배)", "총 추가근무수당 대상 시간 (1배)", "총 무급시간 (-1배)",
        "연차수당 대상 시간", "소급분 (세전 금액)",
        "일별 근로시간", "근로일 및 근로시간", "휴게시간", "주휴일",
    ]
    rows = [headers]

    for _, r in 정규직_df.iterrows():
        emp_id = r.get("employee_id", "")
        if isinstance(emp_id, float):
            emp_id = "" if str(emp_id) == "nan" else str(int(emp_id))
        else:
            emp_id = str(emp_id) if emp_id is not None else ""
        emp_id_norm = emp_id.strip()

        _, emp_name = _parse_nickname_name(str(r.get("employee_name") or ""))
        if not emp_name:
            emp_name = str(r.get("employee_name") or "")

        ot_pay = int(float(r.get("overtime_pay") or 0))
        ot_hours = round(ot_pay / OT_HOURLY_RATE, 1) if ot_pay else ""
        unpaid_hrs = float(r.get("unpaid_hours") or 0)
        unpaid_str = round(unpaid_hrs, 1) if unpaid_hrs else ""

        work_hours = REGULAR_WORK_HOURS_BY_ID.get(emp_id_norm, "(월~금) 9:00~18:00")

        rows.append([
            emp_id,
            emp_name,
            "",  # 주민등록번호
            "",  # 비고
            ot_hours if ot_hours else "",
            "",  # 총 추가근무수당 대상 시간 (1배)
            unpaid_str if unpaid_str else "",
            "",  # 연차수당 대상 시간
            "",  # 소급분
            "",  # 일별 근로시간
            work_hours,
            "(월~금) 12:00~13:00",  # 휴게시간
            "일요일",  # 주휴일
        ])

    return rows


def build_freelancer_sheet_data(freelancer_df) -> list[list]:
    """
    프리랜스(F사번) 전용: 닉네임, 성명, 주민등록번호, 세전 + 합계 행
    """
    rows = [["닉네임", "성명", "주민등록번호", "세전"]]
    total_pay_sum = 0
    for _, r in freelancer_df.iterrows():
        emp_name = str(r.get("employee_name") or "")
        nickname, name = _parse_nickname_name(emp_name)
        total_pay = int(float(r.get("total_pay") or 0))
        total_pay_sum += total_pay
        rows.append([nickname, name, "", _fmt_num(total_pay)])
    rows.append(["", "", "합계", _fmt_num(total_pay_sum)])
    return rows


def _infer_first_attendance_from_row(full_row, date_cols: list, payroll_year: int, payroll_month: int) -> str | None:
    """날짜 컬럼(1/26, 2/5 등) 중 산정기간 내에서 값이 있는 첫 날을 YYYY-MM-DD로 반환."""
    start, end = _payroll_period(payroll_year, payroll_month)
    parsed = []
    for col in date_cols:
        try:
            m, d = str(col).strip().split("/")
            m, d = int(m), int(d)
            y = payroll_year - 1 if m > payroll_month else payroll_year
            dte = date(y, m, d)
        except Exception:
            continue
        if not (start <= dte <= end):
            continue
        v = full_row.get(col)
        if v is None or (isinstance(v, float) and v != v):
            continue
        s = str(v).strip().replace(",", ".")
        if not s or s.lower() == "nan":
            continue
        try:
            float(s)
        except (TypeError, ValueError):
            continue
        parsed.append((dte, f"{dte.year:04d}-{dte.month:02d}-{dte.day:02d}"))
    if not parsed:
        return None
    parsed.sort(key=lambda x: x[0])
    return parsed[0][1]


def build_sheet_data(
    payroll_df,
    payroll_year: int,
    payroll_month: int,
    meal_allowance: int = MEAL_ALLOWANCE,
    payroll_full: "pd.DataFrame | None" = None,
) -> list[list]:
    """
    payroll_result DataFrame → 구글 시트용 2차원 리스트
    기본급: (11,000 × scheduled_minutes/60 × 영업일) - 200,000(식대), 계약별
    payroll_full 있으면 사번 없음(미지정)의 first_attendance_date 없을 때 날짜 컬럼에서 추정.
    """
    import re
    contract_types, employee_contracts = _load_contract_config()
    date_cols = []
    if payroll_full is not None:
        date_cols = [c for c in payroll_full.columns if re.match(r"^\d{1,2}/\d{1,2}$", str(c).strip())]
    rows = []

    # 1행: 합계 행 (지금은 빈값, 사용자가 수동 입력 가능)
    header1 = ["사원코드", "사원명", "주민등록번호", "비고", "부서", "직급"] + [""] * len(PAY_COLUMNS)
    header1 += [""] * len(DEDUCT_COLUMNS)
    header1.append("차인지급액")
    rows.append(header1)

    # 2행: 컬럼명
    header2 = ["사원코드", "사원명", "주민등록번호", "비고", "부서", "직급"] + PAY_COLUMNS + DEDUCT_COLUMNS + ["차인지급액"]
    rows.append(header2)

    # 데이터 행
    for _, r in payroll_df.iterrows():
        emp_id = r.get("employee_id", "")
        emp_name = r.get("employee_name", "")
        if isinstance(emp_id, float):
            emp_id = "" if str(emp_id) == "nan" else str(int(emp_id))
        else:
            emp_id = str(emp_id) if emp_id is not None else ""
        emp_name = str(emp_name) if emp_name is not None and str(emp_name) != "nan" else ""

        # 기본급: 사번 없음(미지정)은 최초 출근일~종료일 영업일. first_attendance_date 없으면 날짜 컬럼에서 추정
        first_date_raw = r.get("first_attendance_date")
        first_date_str = ""
        if first_date_raw is not None and str(first_date_raw).strip() and str(first_date_raw).strip().lower() != "nan":
            first_date_str = str(first_date_raw).strip()[:10]
        use_first_date = None
        if str(emp_id).strip().startswith("미지정"):
            if first_date_str:
                use_first_date = first_date_str
            elif payroll_full is not None and date_cols and r.name in payroll_full.index:
                use_first_date = _infer_first_attendance_from_row(
                    payroll_full.loc[r.name], date_cols, payroll_year, payroll_month
                )
        base_pay = _calc_base_pay_for_employee(
            emp_id or r.get("employee_id"), payroll_year, payroll_month,
            contract_types, employee_contracts,
            first_attendance_date=use_first_date,
        )

        # 기본 정보 (6열)
        row = [emp_id, emp_name, "", "", "", ""]

        # 지급 항목
        ot = int(float(r.get("overtime_pay") or 0))
        wa = int(float(r.get("weekly_allowance_pay") or 0))
        unpaid_hrs = float(r.get("unpaid_hours") or 0)
        unpaid_leave_amt = int(unpaid_hrs * -UNPAID_HOURLY_RATE)  # 음수 (예: 8시간 → -88,000)
        # 육아수당(125 등): 200,000원을 기본급에서 차감·육아수당 항목으로 표시. 지급액계 = 기본급+식대+육아수당+주휴+야근+무급
        emp_id_norm = str(emp_id).strip() if emp_id else ""
        childcare_allowance = CHILDCARE_ALLOWANCE_AMOUNT if emp_id_norm in CHILDCARE_ALLOWANCE_EMPLOYEE_IDS else 0
        base_pay_display = base_pay - childcare_allowance if childcare_allowance else base_pay  # 기본급 컬럼에 표시할 값
        total = base_pay_display + meal_allowance + (childcare_allowance or 0) + ot + wa + unpaid_leave_amt  # 지급액계 = 표시 기본급+식대+육아수당+야근+주휴+무급

        for col in PAY_COLUMNS:
            if col == "기본급":
                row.append(_fmt_num(base_pay_display))
            elif col == "식대" and meal_allowance:
                row.append(_fmt_num(meal_allowance))
            elif col == "육아수당":
                row.append(_fmt_num(childcare_allowance) if childcare_allowance else "")
            elif col == "무급휴가":
                row.append(_fmt_num(unpaid_leave_amt))
            elif col == "지급액계":
                row.append(_fmt_num(total))
            else:
                src = PAY_MAPPING.get(col)
                val = r.get(src) if src else None
                row.append(_fmt_num(val))

        # 공제 항목 (빈칸)
        for _ in DEDUCT_COLUMNS:
            row.append("")

        row.append("")  # 차인지급액
        rows.append(row)

    return rows


# 통상시급 (텍스트_* 생성용)
HOURLY_RATE = 11_000
OT_RATE = HOURLY_RATE * 1.5  # 16500


def _text_기본급_from_contract(
    employee_id,
    payroll_year: int,
    payroll_month: int,
    contract_types: dict,
    employee_contracts: dict,
    first_attendance_date: str | date | None = None,
) -> str:
    """텍스트_기본급: 시간 = scheduled_minutes/60 × 영업일 수, 금액 = 시간 × 11,000.
    first_attendance_date가 있으면(사번 없는 인원 등) 해당일~기간 종료일 기준 영업일."""
    emp_key = str(employee_id or "").strip()
    try:
        if isinstance(employee_id, (int, float)) and str(employee_id) != "nan":
            emp_key = str(int(employee_id))
    except (ValueError, TypeError):
        pass
    ctype = (
        employee_contracts.get(emp_key)
        or employee_contracts.get("default")
        or "standard_9to6"
    )
    defn = contract_types.get(ctype, {"scheduled_minutes": 480, "weekdays": [0, 1, 2, 3, 4]})
    scheduled_min = int(defn.get("scheduled_minutes", 480))
    weekdays = defn.get("weekdays", [0, 1, 2, 3, 4])
    start, end = _payroll_period(payroll_year, payroll_month)
    if first_attendance_date:
        try:
            start_override = (
                first_attendance_date
                if isinstance(first_attendance_date, date)
                else date(*[int(x) for x in str(first_attendance_date).strip()[:10].split("-")])
            )
            if start_override > start:
                start = start_override
        except Exception:
            pass
    days = _count_business_days(start, end, weekdays)
    hrs = (scheduled_min / 60) * days
    amount = int(hrs * HOURLY_RATE)
    return f"기본급 : {hrs:.0f}시간 x 통상시급 = {amount:,}원"


def _text_주휴수당(wa_pay: float) -> str:
    if not wa_pay or wa_pay <= 0:
        return ""
    hrs = wa_pay / HOURLY_RATE
    return f"주휴수당 : {hrs:.1f}시간 x 통상시급 = {int(wa_pay):,}원"


def _text_야근수당(ot_pay: float) -> str:
    if not ot_pay or ot_pay <= 0:
        return ""
    hrs = ot_pay / OT_RATE
    return f"야근수당 : {hrs:.1f}시간 x 통상시급 x 1.5 = {int(ot_pay):,}원"


def _text_무급휴가(unpaid_hrs: float) -> str:
    if not unpaid_hrs or unpaid_hrs <= 0:
        return ""
    amt = int(unpaid_hrs * HOURLY_RATE)
    return f"무급휴가 : {unpaid_hrs:.0f}시간 x 통상시급 x (-1) = -{amt:,}원"


def _text_추가근무수당(extra_pay: float) -> str:
    """연장 1배 수당. payroll에 해당 항목 있으면 사용."""
    if not extra_pay or extra_pay <= 0:
        return ""
    hrs = extra_pay / HOURLY_RATE
    return f"추가근무수당 : {hrs:.1f}시간 x 통상시급 = {int(extra_pay):,}원"


def _strip_equals_amount_suffix(text: str) -> str:
    """문자열 끝의 '= {금액}원' 꼬리만 제거."""
    t = str(text or "").strip()
    if " = " not in t:
        return t
    left, right = t.rsplit(" = ", 1)
    if not right.endswith("원"):
        return t
    amt = right[:-1].strip().replace(",", "")
    if amt.startswith("-"):
        amt = amt[1:]
    if amt.isdigit():
        return left.strip()
    return t


def _emp_contract_type(eid, employee_contracts: dict) -> str:
    """사번으로 계약 유형 반환."""
    emp_key = str(eid or "").strip()
    try:
        if isinstance(eid, (int, float)) and str(eid) != "nan":
            emp_key = str(int(eid))
    except (ValueError, TypeError):
        pass
    return (
        employee_contracts.get(emp_key)
        or employee_contracts.get("default")
        or "standard_9to6"
    )


def _mtw_thu_fri_extra_hours(r_full: "pd.Series", date_cols: list, payroll_year: int, payroll_month: int) -> float:
    """freelancer_9to6_MTW: 산정기간 내 목(3)·금(4) 요일 셀의 근무시간 합 → 추가근무 시간(시간 단위)."""
    import re
    start, end = _payroll_period(payroll_year, payroll_month)
    total = 0.0
    for col in date_cols:
        try:
            m, d = str(col).strip().split("/")
            m, d = int(m), int(d)
            y = payroll_year - 1 if m > payroll_month else payroll_year
            dte = date(y, m, d)
        except Exception:
            continue
        if not (start <= dte <= end) or dte.weekday() not in (3, 4):
            continue
        v = r_full.get(col)
        if v is None or (isinstance(v, float) and v != v) or v == "":
            continue
        try:
            s = str(v).strip().replace(",", ".")
            if not s:
                continue
            total += float(s)
        except (TypeError, ValueError):
            pass
    return total


def build_email_sheet_data(
    payroll_all: "pd.DataFrame",
    payroll_year: int,
    payroll_month: int,
    is_freelancer_fn,
    is_regular_fn,
) -> list[list]:
    """
    이메일_발송용_정보 시트 데이터.
    컬럼: 타입, 성명, 타입, 주민등록번호, 이메일주소, 첨부파일,
          텍스트_기본급, 텍스트_주휴수당, 텍스트_야근수당, 텍스트_무급휴가, 텍스트_추가근무수당
    payroll_all에는 날짜 컬럼(예: 1/26, 2/5)이 포함되어야 하며, freelancer_9to6_MTW 인원은
    목·금 근무시간이 텍스트_추가근무수당에 반영된다.
    """
    import re
    contract_types, employee_contracts = _load_contract_config()
    yy = payroll_year % 100
    mm = payroll_month
    yyyymm = f"{payroll_year}{mm:02d}"

    headers = [
        "사번",
        "타입",
        "성명",
        "타입",
        "주민등록번호",
        "이메일주소",
        "첨부파일",
        "텍스트_기본급",
        "텍스트_주휴수당",
        "텍스트_야근수당",
        "텍스트_무급휴가",
        "텍스트_추가근무수당",
    ]
    need_cols = ["employee_id", "employee_name", "base_pay", "overtime_pay", "weekly_allowance_pay", "unpaid_hours"]
    date_cols = [c for c in payroll_all.columns if re.match(r"^\d{1,2}/\d{1,2}$", str(c).strip())]
    payroll = payroll_all[[c for c in need_cols if c in payroll_all.columns]].copy()
    if "extra_overtime_pay" in payroll_all.columns:
        payroll["extra_pay"] = payroll_all["extra_overtime_pay"].values
    else:
        payroll["extra_pay"] = 0

    data_rows = []
    for i, r in payroll.iterrows():
        r_full = payroll_all.loc[i] if i in payroll_all.index else r
        eid = r.get("employee_id", "")
        eid_str = str(int(eid)) if isinstance(eid, (int, float)) and not (isinstance(eid, float) and str(eid) == "nan") else str(eid or "").strip()
        _, real_name = _parse_nickname_name(str(r.get("employee_name") or ""))

        # 타입1: 정규직/상용직/프리랜서
        if is_freelancer_fn(eid):
            type1 = "프리랜서"
        elif is_regular_fn(eid):
            type1 = "정규직"
        else:
            type1 = "상용직"

        # 타입2: 정규직/상용직/프리랜스
        if is_freelancer_fn(eid):
            type2 = "프리랜스"
        elif is_regular_fn(eid):
            type2 = "정규직"
        else:
            type2 = "상용직"

        email = ""
        ssn = ""

        # 첨부파일: 급여명세서(근로기준1)_{id}_{name}_{YYYYMM}.pdf (프리랜서도 동일 형식)
        attach = f"급여명세서(근로기준1)_{eid_str}_{real_name}_{yyyymm}.pdf"
        if is_freelancer_fn(eid) and not real_name:
            attach = f"{eid_str}_{yyyymm}.pdf"

        ot_pay = float(r.get("overtime_pay") or 0)
        wa_pay = float(r.get("weekly_allowance_pay") or 0)
        unpaid_hrs = float(r.get("unpaid_hours") or 0)
        extra_pay = float(r.get("extra_pay") or 0)

        # freelancer_9to6_MTW: 목·금 근무시간을 추가근무수당(1배)으로 텍스트_추가근무수당에 반영
        ctype = _emp_contract_type(eid, employee_contracts)
        if ctype == "freelancer_9to6_MTW" and date_cols:
            mtw_hrs = _mtw_thu_fri_extra_hours(r_full, date_cols, payroll_year, payroll_month)
            extra_pay += mtw_hrs * HOURLY_RATE

        # 정규직은 텍스트_기본급/텍스트_주휴수당 비움. 상용직/프리랜서는 전부 채움. 사번 없음은 최초 출근일~종료일 영업일
        is_regular = is_regular_fn(eid)
        fill_text = not is_regular
        first_date = r_full.get("first_attendance_date") or ""
        use_first_date = first_date if (eid_str.startswith("미지정") and first_date) else None
        t_base = (
            _text_기본급_from_contract(
                eid, payroll_year, payroll_month, contract_types, employee_contracts,
                first_attendance_date=use_first_date,
            )
            if fill_text
            else ""
        )
        t_wa = _text_주휴수당(wa_pay) if fill_text else ""
        t_ot = _text_야근수당(ot_pay)
        t_unpaid = _text_무급휴가(unpaid_hrs)
        t_extra = _text_추가근무수당(extra_pay)
        if is_regular:
            t_base = _strip_equals_amount_suffix(t_base)
            t_wa = _strip_equals_amount_suffix(t_wa)
            t_ot = _strip_equals_amount_suffix(t_ot)
            t_unpaid = _strip_equals_amount_suffix(t_unpaid)
            t_extra = _strip_equals_amount_suffix(t_extra)

        sort_key = 0 if type1.startswith("정규직") else (1 if type1.startswith("상용직") else 2)
        data_rows.append((sort_key, [eid_str, type1, real_name, type2, ssn, email, attach, t_base, t_wa, t_ot, t_unpaid, t_extra]))

    data_rows.sort(key=lambda x: x[0])
    rows = [headers] + [row for _, row in data_rows]
    return rows


def _infer_payroll_month(payroll: "pd.DataFrame") -> tuple[int, int]:
    """날짜 컬럼(예: 1/26, 2/5)에서 지급월 추정. 마지막 날짜 기준."""
    import re
    date_cols = [c for c in payroll.columns if re.match(r"^\d{1,2}/\d{1,2}$", str(c).strip())]
    if not date_cols:
        # fallback: 당월
        from datetime import datetime
        now = datetime.now()
        return now.year, now.month
    # "1/26" → (1, 26), "2/5" → (2, 5)
    def parse(d):
        try:
            m, d = str(d).strip().split("/")
            return int(m), int(d)
        except Exception:
            return 0, 0
    # 마지막 컬럼 기준 지급월 추정
    last_col = max(date_cols, key=lambda c: (parse(c)[0], parse(c)[1]))
    m, d = parse(last_col)
    now = __import__("datetime").datetime.now()
    year = now.year
    if m < now.month - 1:
        year += 1  # 12월 데이터인데 현재 1월인 경우 등
    return year, m


def create_google_sheet(
    output_dir: Path,
    title: str = "급여대장",
    meal_allowance: int = MEAL_ALLOWANCE,
    payroll_year: int | None = None,
    payroll_month: int | None = None,
) -> str:
    """
    payroll_result.csv를 읽어 구글 시트를 생성하고 URL 반환.
    기본급: 급여산정일(전월25~당월24) 영업일 × 88,000 - 200,000(식대)
    """
    try:
        import pandas as pd
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError as e:
        raise RuntimeError(
            "구글 시트 내보내기를 위해 gspread, google-auth 패키지가 필요합니다. "
            "pip install gspread google-auth"
        ) from e

    csv_path = output_dir / "payroll_result.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"payroll_result.csv를 찾을 수 없습니다: {output_dir}")

    payroll = pd.read_csv(csv_path, encoding="utf-8-sig")
    payroll.columns = [str(c).strip().lstrip("\ufeff") for c in payroll.columns]

    # 지급월 추정 (날짜 컬럼 1/26, 2/5 등에서) 또는 전달값 사용
    if payroll_year is None or payroll_month is None:
        py, pm = _infer_payroll_month(payroll)
        payroll_year = payroll_year if payroll_year is not None else py
        payroll_month = payroll_month if payroll_month is not None else pm

    # 상용직 / 정규직 / 프리랜스(F사번) 분리
    def _norm_id(eid) -> str:
        s = str(eid or "").strip()
        try:
            f = float(s)
            return str(int(f)) if f == int(f) else s
        except (ValueError, TypeError):
            return s

    def is_freelancer(eid) -> bool:
        return _norm_id(eid).upper().startswith("F")

    def is_regular(eid) -> bool:
        return _norm_id(eid) in REGULAR_EMPLOYEE_IDS

    need_cols = ["employee_id", "employee_name", "overtime_pay", "weekly_allowance_pay", "unpaid_hours", "total_pay"]
    extra_cols = [c for c in ["first_attendance_date"] if c in payroll.columns]
    payroll_all = payroll[[c for c in need_cols if c in payroll.columns] + extra_cols]
    payroll_freelance = payroll_all[payroll_all["employee_id"].apply(is_freelancer)]
    payroll_regular_staff = payroll_all[
        ~payroll_all["employee_id"].apply(is_freelancer) & ~payroll_all["employee_id"].apply(is_regular)
    ]  # 상용직: F 제외, 정규직 제외
    payroll_regular = payroll_all[payroll_all["employee_id"].apply(is_regular)]  # 정규직

    data_regular = build_sheet_data(
        payroll_regular_staff, payroll_year, payroll_month, meal_allowance=meal_allowance,
        payroll_full=payroll,
    )
    data_정규직 = build_정규직_sheet_data(payroll_regular)
    data_freelance = build_freelancer_sheet_data(payroll_freelance)
    data_email = build_email_sheet_data(
        payroll, payroll_year, payroll_month, is_freelancer, is_regular
    )

    sheet_title_regular = f"{payroll_year}년{payroll_month:02d}월_상용직"
    sheet_title_정규직 = f"{payroll_year}년{payroll_month:02d}월_정규직"
    sheet_title_freelance = f"{payroll_year}년{payroll_month:02d}월_프리랜스"

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    json_raw = (os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON") or "").strip()
    # JSON으로 인증할 때도 GOOGLE_APPLICATION_CREDENTIALS(파일 경로)가 남아 있으면
    # google-auth 등이 ADC로 그 경로를 읽으려다 컨테이너에서 PermissionError가 날 수 있음 → 임시 제거
    _saved_gac_path = None
    if json_raw:
        _saved_gac_path = os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)

    try:
        with _google_api_safe_env():
            if json_raw:
                try:
                    info = json.loads(json_raw)
                except json.JSONDecodeError as e:
                    raise RuntimeError(
                        "GOOGLE_APPLICATION_CREDENTIALS_JSON 파싱에 실패했습니다. "
                        "Railway에는 서비스 계정 JSON 전체를 한 덩어리로 넣고, 앞뒤에 따옴표를 붙이지 마세요."
                    ) from e
                if not isinstance(info, dict):
                    raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS_JSON은 JSON 객체여야 합니다.")
                for key in ("type", "project_id", "private_key", "client_email"):
                    if not info.get(key):
                        raise RuntimeError(
                            f"GOOGLE_APPLICATION_CREDENTIALS_JSON에 필수 필드 '{key}'가 없습니다. "
                            "서비스 계정 키 JSON 전체를 복사했는지 확인하세요."
                        )
                try:
                    creds = _credentials_from_service_account_dict(info, scopes)
                except PermissionError as e:
                    logger.exception("service_account dict → temp file")
                    fn = getattr(e, "filename", None)
                    raise RuntimeError(
                        f"자격 증명 처리 중 PermissionError{f': {fn}' if fn else ''}. "
                        "Railway Variables에서 GOOGLE_APPLICATION_CREDENTIALS(파일 경로)를 삭제하고 "
                        "SSL_CERT_FILE·REQUESTS_CA_BUNDLE 등 로컬 전용 경로 변수도 비우세요."
                    ) from e
                except Exception as e:
                    raise RuntimeError(
                        f"서비스 계정 자격 증명을 읽지 못했습니다 ({type(e).__name__}: {e}). "
                        "private_key가 잘리지 않았는지 확인하세요."
                    ) from e
            else:
                cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
                if not cred_path or not Path(cred_path).is_file():
                    for p in [
                        ROOT / "credentials" / "service_account.json",
                        ROOT / ".keys" / "ops-robot-keys.json",
                    ]:
                        if p.is_file():
                            cred_path = str(p)
                            break
                    else:
                        raise RuntimeError(
                            "구글 시트 API 인증이 없습니다. "
                            "Railway에는 GOOGLE_APPLICATION_CREDENTIALS_JSON에 서비스 계정 JSON 전체를 넣으세요. "
                            "로컬 경로용 GOOGLE_APPLICATION_CREDENTIALS(파일 경로)는 Railway에서 제거하거나 비우세요."
                        )
                try:
                    creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
                except PermissionError as e:
                    fn = getattr(e, "filename", None) or cred_path
                    raise RuntimeError(
                        f"키 파일을 읽을 수 없습니다(PermissionError): {fn}. "
                        "Railway에서는 GOOGLE_APPLICATION_CREDENTIALS_JSON만 사용하고, "
                        "GOOGLE_APPLICATION_CREDENTIALS에 Windows 경로 등 읽기 불가 경로가 들어가 있지 않은지 확인하세요."
                    ) from e

            try:
                gc = gspread.authorize(creds)
                sh = gc.open_by_key(TARGET_SPREADSHEET_ID)
            except PermissionError as e:
                logger.exception("gspread authorize / open_by_key")
                fn = getattr(e, "filename", None)
                raise RuntimeError(
                    f"PermissionError{f' ({fn})' if fn else ''}. "
                    "GOOGLE_APPLICATION_CREDENTIALS_JSON만 두고 파일 경로 변수는 삭제. "
                    "SSL_CERT_FILE 등 인증서 경로가 있으면 제거 후 재시도."
                ) from e
            except Exception as e:
                hint = (
                    f"구글 시트 API 오류 ({type(e).__name__}): {e}. "
                    "① GOOGLE_APPLICATION_CREDENTIALS_JSON(서비스 계정 JSON 전체) "
                    "② client_email을 스프레드시트에 편집자로 공유 "
                    "③ Google Cloud에서 'Google Sheets API' 사용 설정"
                )
                raise RuntimeError(hint) from e

            # payroll_result 원본 시트 (화면에서 수정 반영된 데이터 그대로 반출)
            sheet_title_payroll = f"{payroll_year}년{payroll_month:02d}월_payroll_result"
            payroll_header = [str(c) for c in payroll.columns]
            payroll_data_rows = []
            for _, r in payroll.iterrows():
                payroll_data_rows.append(
                    [r[c] if c in r.index and pd.notna(r[c]) else "" for c in payroll.columns]
                )
            data_payroll_result = [payroll_header] + payroll_data_rows
            rows_p = max(len(data_payroll_result) + 10, 100)
            cols_p = max(len(payroll_header), 20)
            ws_payroll = sh.add_worksheet(title=sheet_title_payroll, rows=rows_p, cols=cols_p)
            ws_payroll.update("A1", data_payroll_result, value_input_option="USER_ENTERED")

            # 상용직 시트 생성
            rows_count = max(len(data_regular) + 10, 100)
            cols_count = max(len(data_regular[0]) if data_regular else 50, 50)
            ws_regular = sh.add_worksheet(title=sheet_title_regular, rows=rows_count, cols=cols_count)
            ws_regular.update("A1", data_regular, value_input_option="USER_ENTERED")

            # 정규직 시트 생성 (상용직과 동일 형식)
            rows_j = max(len(data_정규직) + 10, 100)
            cols_j = max(len(data_정규직[0]) if data_정규직 else 50, 50)
            ws_정규직 = sh.add_worksheet(title=sheet_title_정규직, rows=rows_j, cols=cols_j)
            ws_정규직.update("A1", data_정규직, value_input_option="USER_ENTERED")

            # 프리랜스 시트 생성 (닉네임, 성명, 주민등록번호, 세전)
            rows_f = max(len(data_freelance) + 10, 50)
            cols_f = max(len(data_freelance[0]) if data_freelance else 4, 4)
            ws_freelance = sh.add_worksheet(title=sheet_title_freelance, rows=rows_f, cols=cols_f)
            ws_freelance.update("A1", data_freelance, value_input_option="USER_ENTERED")

            # 이메일_발송용_정보 시트 생성
            sheet_title_email = f"{payroll_year}년{payroll_month:02d}월_이메일_발송용_정보"
            rows_e = max(len(data_email) + 10, 50)
            cols_e = max(len(data_email[0]) if data_email else 11, 11)
            ws_email = sh.add_worksheet(title=sheet_title_email, rows=rows_e, cols=cols_e)
            ws_email.update("A1", data_email, value_input_option="USER_ENTERED")

            # 상용직 시트로 이동하는 URL
            url = f"https://docs.google.com/spreadsheets/d/{TARGET_SPREADSHEET_ID}/edit#gid={ws_regular.id}"
            return url
    finally:
        # JSON으로 내보낸 경우 잘못된 파일 경로를 복구하지 않음(다음 요청에서 다시 PermissionError 방지)
        if _saved_gac_path is not None and not json_raw:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _saved_gac_path
