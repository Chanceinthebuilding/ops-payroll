from __future__ import annotations

import re
import pandas as pd
import yaml
from datetime import datetime, timedelta, time
from pathlib import Path
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
ROOT = Path(__file__).resolve().parent
CONTRACT_CONFIG_PATH = ROOT / "contract_config.yaml"


# =========================
# input 파일 자동 찾기
# =========================
def find_input_file() -> Path:
    if not INPUT_DIR.exists():
        raise Exception("input 폴더가 없습니다.")
    # 엑셀 잠금 파일(~$로 시작) 제외
    files = [f for f in INPUT_DIR.glob("*.xlsx") if not f.name.startswith("~$")]
    if not files:
        raise Exception("input 폴더에 xlsx 파일이 없습니다.")
    # 1개면 그대로, 여러 개면 수정일 기준 가장 최근 것
    return max(files, key=lambda f: f.stat().st_mtime)


# =========================
# 시프티 엑셀 로드
# =========================
def load_shiftie(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)

    required = ["사원번호", "직원", "날짜", "출근시간", "퇴근시간"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise Exception(f"필수 컬럼 없음: {missing}")

    # 휴게시간 없으면 0으로 채움 (근무시간만 필요 시 생략 가능)
    if "휴게시간" not in df.columns:
        df["휴게시간"] = 0

    return df


# =========================
# 날짜+시간 결합
# =========================
def _to_time(val) -> time | None:
    if pd.isna(val) or val is None or str(val).strip() == "":
        return None

    # 엑셀이 time 객체로 주는 경우
    if isinstance(val, time):
        return val

    # pandas Timestamp로 들어오는 경우
    try:
        ts = pd.to_datetime(val)
        return ts.to_pydatetime().time()
    except Exception:
        return None


def combine_dt(date_val, time_val):
    t = _to_time(time_val)
    if t is None:
        return None
    d = pd.to_datetime(date_val).date()
    return datetime.combine(d, t, tzinfo=KST)


# =========================
# 출근시간 정규화: 08:00~09:15 → 09:00
# =========================
CHECKIN_NORMALIZE_START = time(8, 0)    # 08:00 포함
CHECKIN_NORMALIZE_END = time(9, 15)     # 09:15 포함
CHECKIN_NORMALIZED = time(9, 0)


def normalize_checkin(dt: datetime | None) -> datetime | None:
    """출근시간이 08:00~09:15 구간이면 해당일 09:00으로 통일."""
    if dt is None:
        return None
    t = dt.astimezone(KST).time() if dt.tzinfo else dt.time()
    if CHECKIN_NORMALIZE_START <= t <= CHECKIN_NORMALIZE_END:
        return datetime.combine(dt.date(), CHECKIN_NORMALIZED, tzinfo=KST)
    return dt


# =========================
# 퇴근시간 정규화: 17:45~18:15 → 18:00 (아래 n시 정규화에 포함)
# =========================
CHECKOUT_NORMALIZE_START = time(17, 45)
CHECKOUT_NORMALIZE_END = time(18, 15)
CHECKOUT_NORMALIZED = time(18, 0)


def normalize_checkout(dt: datetime | None) -> datetime | None:
    """퇴근시간이 17:45~18:15 구간이면 해당일 18:00으로 통일."""
    if dt is None:
        return None
    t = dt.astimezone(KST).time() if dt.tzinfo else dt.time()
    if CHECKOUT_NORMALIZE_START <= t <= CHECKOUT_NORMALIZE_END:
        return datetime.combine(dt.date(), CHECKOUT_NORMALIZED, tzinfo=KST)
    return dt


# =========================
# n시 정규화: (n-1):45 ~ n:15 → n:00 (n = 10~17). 17:45~18:15는 퇴근 정규화로 18:00
# =========================
def normalize_to_n_hour(dt: datetime | None) -> datetime | None:
    """(n-1):45 부터 n:15까지는 n시로 간주. n은 10~17."""
    if dt is None:
        return None
    t = dt.astimezone(KST).time() if dt.tzinfo else dt.time()
    for n in range(10, 18):  # 10 ~ 17
        start = time(n - 1, 45)
        end = time(n, 15)
        if start <= t <= end:
            return datetime.combine(dt.date(), time(n, 0), tzinfo=KST)
    return dt


# =========================
# 휴게시간 파싱 (현업용)
# =========================
_BREAK_HHMM_RE = re.compile(r"^\s*(\d{1,3})\s*:\s*(\d{1,2})\s*$")
_BREAK_KO_RE = re.compile(r"^\s*(?:(\d+)\s*시간)?\s*(?:(\d+)\s*분)?\s*$")
_BREAK_MIN_RE = re.compile(r"^\s*(\d+)\s*분\s*$")


def parse_break_minutes(val) -> int:
    """
    지원 포맷:
    - 90분
    - 1시간 30분 / 1시간 / 0분
    - 593시간 30분 (합계 등)
    - HH:MM, H:MM (예: 01:30, 1:30)
    - 엑셀 time 타입 (datetime.time)
    - 숫자만: 분으로 간주
    그 외: 0
    """
    if pd.isna(val) or val is None:
        return 0

    # 엑셀 time 타입(01:30 등)
    if isinstance(val, time):
        return val.hour * 60 + val.minute

    s = str(val).strip()
    if s == "":
        return 0

    # HH:MM / H:MM
    m = _BREAK_HHMM_RE.match(s)
    if m:
        hh = int(m.group(1))
        mm = int(m.group(2))
        if 0 <= mm < 60:
            return hh * 60 + mm
        return 0

    # "90분"
    m = _BREAK_MIN_RE.match(s)
    if m:
        return int(m.group(1))

    # "1시간 30분" / "1시간" / "30분" / "593시간 30분"
    m = _BREAK_KO_RE.match(s)
    if m:
        hh = int(m.group(1)) if m.group(1) else 0
        mm = int(m.group(2)) if m.group(2) else 0
        # "593시간 30분" 같은 합계도 안전하게 분으로 변환됨
        if 0 <= mm < 60:
            return hh * 60 + mm
        # 간혹 "1시간 90분" 같이 이상한 값이 들어오면 0 처리
        return 0

    # 숫자만 있으면 분으로 처리
    if s.isdigit():
        return int(s)

    return 0


# =========================
# 휴게시간 산정 (단일 규칙)
# =========================
# 끝시간 - 시작시간(차)가 4시간 이하 → 휴게 없음
# 차가 5시간 이상 → 휴게 1시간 (실근무 = 차 - 1h)
# 예: 09-13(4h)→4h, 09-14(5h)→4h, 09-16(7h)→6h, 11-18(7h)→6h, 09-18(9h)→8h
BREAK_THRESHOLD_HOURS = 5
BREAK_MINUTES_WHEN_APPLIED = 60


def compute_work_and_break(
    start: datetime, end: datetime, break_minutes_when_applied: int = BREAK_MINUTES_WHEN_APPLIED
) -> tuple[int, int]:
    """
    구간(차) 기준 휴게 산정. 반환값은 (총 구간 분, 휴게 분).
    - 차 <= 4시간: 휴게 0
    - 차 >= 5시간: 휴게 = break_minutes_when_applied (계약별, 예: standard_9to4.5 → 30분)
    """
    span_minutes = int((end - start).total_seconds() // 60)
    span_hours = span_minutes / 60.0

    if span_hours < BREAK_THRESHOLD_HOURS:
        return span_minutes, 0
    return span_minutes, break_minutes_when_applied


# =========================
# 세그먼트 생성
# =========================
def build_segments(df: pd.DataFrame) -> pd.DataFrame:
    contract_types, employee_contracts = load_contract_config()

    rows = []
    df2 = df.copy()
    df2 = df2[~df2["날짜"].isna()]
    # 사번 없음(전월 미포함 인원)도 포함 — placeholder ID 부여(동일 직원명 = 동일 ID)
    no_id_name_to_id: dict[str, str] = {}
    no_id_counter = 0

    for idx, r in df2.iterrows():
        raw_emp = r.get("사원번호")
        if pd.isna(raw_emp) or str(raw_emp).strip() == "":
            name = str(r.get("직원", "") or "").strip() or "미지정"
            if name not in no_id_name_to_id:
                no_id_name_to_id[name] = f"미지정_{no_id_counter}"
                no_id_counter += 1
            emp = no_id_name_to_id[name]
        else:
            emp = str(raw_emp).strip()

        start = combine_dt(r["날짜"], r["출근시간"])
        start = normalize_checkin(start)
        start = normalize_to_n_hour(start)
        end = combine_dt(r["날짜"], r["퇴근시간"])
        end = normalize_checkout(end)
        end = normalize_to_n_hour(end)

        if start is None or end is None:
            continue

        if end < start:
            end += timedelta(days=1)

        break_mins = get_break_minutes_for_employee(emp, contract_types, employee_contracts)
        work_min, break_min = compute_work_and_break(start, end, break_minutes_when_applied=break_mins)

        rows.append({
            "employee_id": emp,
            "employee_name": str(r.get("직원") or "").strip() if pd.notna(r.get("직원")) else "",
            "date": pd.to_datetime(r["날짜"]).date(),
            "segment_type": "WORK",
            "start_at": start,
            "end_at": end,
            "minutes": work_min,
            "break_minutes": break_min,
        })

    return pd.DataFrame(rows)


# =========================
# 계약 설정 로드 (contract_config.yaml)
# =========================
def load_contract_config() -> tuple[dict, dict]:
    """(contract_types, employee_contracts) 반환. 파일 없으면 기본값만."""
    if not CONTRACT_CONFIG_PATH.exists():
        return (
            {"standard_9to6": {"scheduled_minutes": 480, "break_minutes": 60, "weekdays": [0, 1, 2, 3, 4]}},
            {"default": "standard_9to6"},
        )
    with open(CONTRACT_CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    types = cfg.get("contract_types", {"standard_9to6": {"scheduled_minutes": 480, "break_minutes": 60, "weekdays": [0, 1, 2, 3, 4]}})
    employees = cfg.get("employee_contracts", {"default": "standard_9to6"})
    return types, employees


def load_no_shifty_attendance() -> dict:
    """
    contract_config.yaml의 no_shifty_attendance: 시프티 엑셀에 없어도 산정기간 내
    평일(한국 평일 공휴일 제외)마다 지정 시간 근무 행을 자동 추가할 사원 목록.
    키: 사원번호 → {employee_name, daily_net_minutes}
    """
    if not CONTRACT_CONFIG_PATH.exists():
        return {}
    with open(CONTRACT_CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    raw = cfg.get("no_shifty_attendance")
    if not raw or not isinstance(raw, dict):
        return {}
    return dict(raw)


def _contract_emp_key(emp) -> str:
    """계약 조회용 직원 키. CSV/엑셀에서 125.0(float)으로 오면 "125"로 통일."""
    s = str(emp).strip()
    try:
        if isinstance(emp, float) and emp == int(emp):
            return str(int(emp))
        f = float(s)
        if f == int(f):
            return str(int(f))
    except (TypeError, ValueError):
        pass
    return s


def get_contract_for_employee(emp, date_val, contract_types: dict, employee_contracts: dict) -> tuple[str, int]:
    """사원 emp, 날짜 date_val에 대한 contract_type과 해당일 scheduled_minutes 반환."""
    emp_key = _contract_emp_key(emp)
    ctype = (
        employee_contracts.get(emp_key)
        or employee_contracts.get(emp)
        or (employee_contracts.get(int(emp)) if isinstance(emp, float) and emp == int(emp) else None)
        or employee_contracts.get("default")
        or "standard_9to6"
    )
    defn = contract_types.get(ctype)
    if not defn:
        return "standard_9to6", 480
    weekday = pd.to_datetime(date_val).weekday()
    weekdays = defn.get("weekdays", [0, 1, 2, 3, 4])
    scheduled = defn["scheduled_minutes"] if weekday in weekdays else 0
    return ctype, scheduled


def get_break_minutes_for_employee(emp, contract_types: dict, employee_contracts: dict) -> int:
    """사원 emp의 계약 휴게시간(분). 5시간 이상 근무 시 이 값이 차감됨."""
    emp_key = _contract_emp_key(emp)
    ctype = (
        employee_contracts.get(emp_key)
        or employee_contracts.get(emp)
        or (employee_contracts.get(int(emp)) if isinstance(emp, float) and emp == int(emp) else None)
        or employee_contracts.get("default")
        or "standard_9to6"
    )
    defn = contract_types.get(ctype)
    if not defn:
        return BREAK_MINUTES_WHEN_APPLIED
    return int(defn.get("break_minutes", BREAK_MINUTES_WHEN_APPLIED))


# =========================
# daily summary 생성
# =========================
def build_daily_summary(seg: pd.DataFrame) -> pd.DataFrame:
    if seg.empty:
        return pd.DataFrame()

    contract_types, employee_contracts = load_contract_config()

    rows = []
    for (emp, d), g in seg.groupby(["employee_id", "date"]):
        work = int(g["minutes"].sum())
        brk = int(g["break_minutes"].sum())
        net = work - brk
        name = g["employee_name"].iloc[0] if "employee_name" in g.columns else ""
        ctype, scheduled = get_contract_for_employee(emp, d, contract_types, employee_contracts)

        rows.append({
            "employee_id": emp,
            "employee_name": name,
            "date": d,
            "contract_type": ctype,
            "scheduled_minutes": scheduled,
            "work_minutes": work,
            "break_minutes": brk,
            "paid_leave_minutes": 0,
            "net_minutes": net,
            "anomalies": [],
        })

    return pd.DataFrame(rows).sort_values(["employee_id", "date"]).reset_index(drop=True)


# =========================
# 실행
# =========================
def main(input_path: Path | None = None, output_dir: Path | None = None):
    out = Path(output_dir) if output_dir is not None else OUTPUT_DIR
    out.mkdir(parents=True, exist_ok=True)

    f = Path(input_path) if input_path is not None else find_input_file()
    print("INPUT:", f)

    df = load_shiftie(f)
    seg = build_segments(df)
    daily = build_daily_summary(seg)

    seg.to_csv(out / "work_segments.csv", index=False, encoding="utf-8-sig")
    daily.to_csv(out / "daily_summary.csv", index=False, encoding="utf-8-sig")

    print("✅ work_segments.csv, daily_summary.csv 생성")


if __name__ == "__main__":
    main()
