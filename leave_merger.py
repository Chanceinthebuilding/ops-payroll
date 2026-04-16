"""
휴가 엑셀(사원번호, 직원, 휴가 유형, 시작 시간, 유급 시간)을 읽어
daily_summary에 유급·무급 휴가를 반영하고, 날짜별 특이사항(day_highlight)을 저장합니다.
- 연차(유급휴가): 유급 반영, 결과에서 연두색
- 오프(무급휴가), 프리랜스 무급휴가: 무급시간 집계, 결과에서 노란색
- 동일 사원·동일 일자에 유급+무급 행이 모두 있으면 한 daily 행에 합산하고 day_highlight=mixed_leave
- 평일 공휴일: 출퇴근 기록 없어도 계약별 유급 시간 지급; 당일 근무한 경우 휴일 지급 + 실제 근무 시간(야근 반영)
"""
import re
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from attendance_normalizer import (
    get_contract_for_employee,
    load_contract_config,
    load_no_shifty_attendance,
)


# "8시간 00분" / "8시간" / "8:30" / "2:00:00 AM" / 엑셀 시간(숫자) 파싱
RE_HOUR_MIN = re.compile(r"(\d+)\s*시간\s*(\d+)\s*분")
RE_HOUR_ONLY = re.compile(r"(\d+)\s*시간")
RE_COLON = re.compile(r"(\d+)\s*:\s*(\d+)")  # 8:00, 8:30
# 엑셀에서 유급시간이 시간값으로 읽힐 때: "2:00:00 AM", "8:00:00 AM" → 시·분을 그대로 유급 시간(분)으로 사용
RE_TIME_AMPM = re.compile(r"(\d+)\s*:\s*(\d+)(?:\s*:\s*(\d+))?\s*(AM|PM)?", re.I)


def _to_date(d) -> date:
    """pandas Timestamp / datetime / date / str → date."""
    if d is None or (isinstance(d, float) and pd.isna(d)):
        raise ValueError("Invalid date")
    if hasattr(d, "to_pydatetime"):
        return d.to_pydatetime().date()
    if hasattr(d, "date") and callable(getattr(d, "date")):
        return d.date()
    if isinstance(d, str):
        return pd.to_datetime(d[:10]).date()
    return d


def _is_freelancer(emp, employee_contracts=None) -> bool:
    """계약이 freelancer_*이거나 사번이 F로 시작하면 프리랜스. 유급휴일 없음."""
    if employee_contracts is None:
        _, employee_contracts = load_contract_config()
    key = str(emp).strip()
    try:
        if isinstance(emp, (int, float)) and str(emp) != "nan":
            key = str(int(emp))
    except (ValueError, TypeError):
        pass
    ctype = (employee_contracts or {}).get(key) or (employee_contracts or {}).get("default")
    if ctype and str(ctype).startswith("freelancer_"):
        return True
    return str(emp).strip().upper().startswith("F")


def _date_to_dkey(d) -> str:
    """date/Timestamp/str → 'YYYY-MM-DD'. NaN/None → '1900-01-01' (매칭 제외)."""
    try:
        if d is None or (isinstance(d, float) and pd.isna(d)):
            return "1900-01-01"
        dt = _to_date(d)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        s = str(d)
        return s[:10] if len(s) >= 10 else "1900-01-01"


# 알려진 한국 평일 공휴일 (연도, 월, 일) — holidays 라이브러리에 없거나 누락 시 폴백
# 대체공휴일(주말과 겹친 공휴일의 다음 평일 등)도 평일 유급·프리랜서 야근 산정에 포함해야 함
_KNOWN_KR_WEEKDAY_HOLIDAYS = [
    (2026, 2, 16), (2026, 2, 17), (2026, 2, 18),  # 2026 설날 연휴
    (2026, 3, 2),  # 2026 삼일절 대체공휴일 (평일 휴일)
]


def get_weekday_public_holidays_kr(start_date: date, end_date: date) -> set[date]:
    """한국 공휴일 중 평일(월~금)만 반환. holidays 라이브러리 + 알려진 날짜 폴백."""
    start_date = _to_date(start_date)
    end_date = _to_date(end_date)
    out = set()
    # 1) holidays 라이브러리
    try:
        import holidays
        kr = holidays.SouthKorea(years=[start_date.year, end_date.year])
        d = start_date
        while d <= end_date:
            if d.weekday() <= 4 and d in kr:
                out.add(d)
            d += timedelta(days=1)
    except Exception:
        pass
    # 2) 알려진 평일 공휴일 폴백 (구간 내만)
    for t in _KNOWN_KR_WEEKDAY_HOLIDAYS:
        try:
            d = date(t[0], t[1], t[2])
            if start_date <= d <= end_date and d.weekday() <= 4:
                out.add(d)
        except (ValueError, IndexError):
            pass
    return out


def _norm_employee_id(x) -> str:
    """사원번호 통일: 101.0 → '101', '101' → '101', 'F102' → 'F102' (merge 매칭용)."""
    s = str(x).strip()
    try:
        f = float(s)
        if f == int(f):
            return str(int(f))
    except (ValueError, TypeError):
        pass
    return s


def _parse_paid_hours(val) -> int:
    """유급 시간 컬럼 값 -> 분. '8시간 00분', '8시간', '8:00', 숫자, datetime 지원."""
    if pd.isna(val):
        return 0
    # 문자열: 전각 숫자 → 반각, 공백 정규화
    s = str(val).strip()
    s = "".join(chr(0x30 + ord(c) - 0xFF10) if "\uFF10" <= c <= "\uFF19" else c for c in s)
    s = re.sub(r"\s+", " ", s)
    # "8시간 00분" / "8시간"
    m = RE_HOUR_MIN.search(s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    m = RE_HOUR_ONLY.search(s)
    if m:
        return int(m.group(1)) * 60
    # "8:00", "8:30" (시:분)
    m = RE_COLON.search(s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    # "2:00:00 AM", "8:00:00 AM" 등 (엑셀에서 시간 셀으로 읽힌 경우 → 시·분을 유급 시간으로 해석)
    m = RE_TIME_AMPM.search(s)
    if m:
        h, mn = int(m.group(1)), int(m.group(2))
        if m.group(4) and m.group(4).upper() == "PM" and h < 12:
            h += 12
        return h * 60 + mn
    # 엑셀 소수(0.333=8h) 또는 숫자(8=8h)
    try:
        x = float(val)
        if 0 <= x < 1:
            return int(round(x * 24 * 60))
        return int(round(x * 60))
    except (ValueError, TypeError):
        pass
    # datetime/timestamp
    try:
        dt = pd.to_datetime(val)
        return int(dt.hour * 60 + dt.minute)
    except Exception:
        return 0


def _find_unpaid_col(df: pd.DataFrame) -> str | None:
    """무급 시간 컬럼 탐색: 무급 시간 > 시간 > 유급 시간 순."""
    for name in ("무급 시간", "무급시간"):
        if name in df.columns:
            return name
    if "시간" in df.columns:
        return "시간"
    for c in df.columns:
        if "무급" in c and "시간" in c:
            return c
    return None


def load_leave_file(path: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    휴가 엑셀 로드.
    - paid: (employee_id, date, paid_leave_minutes) 유급만, 급여 반영용
    - highlights: (employee_id, date, day_highlight) 연두/노란 색용
    - unpaid: (employee_id, date, unpaid_leave_minutes) 무급 사용 시간
    """
    df = pd.read_excel(path)
    df.columns = [str(c).strip().lstrip("\ufeff") for c in df.columns]
    emp_col = "사원번호" if "사원번호" in df.columns else None
    name_col = "직원" if "직원" in df.columns else None
    type_col = "휴가 유형" if "휴가 유형" in df.columns else "휴가유형" if "휴가유형" in df.columns else None
    start_col = "시작 시간" if "시작 시간" in df.columns else "시작시간" if "시작시간" in df.columns else None
    paid_col = None
    for c in df.columns:
        if c in ("유급 시간", "유급시간"):
            paid_col = c
            break
    if paid_col is None:
        for c in df.columns:
            if "유급" in c and "시간" in c:
                paid_col = c
                break
    if paid_col is None:
        for c in df.columns:
            if "유급" in c:
                paid_col = c
                break
    unpaid_col = _find_unpaid_col(df) or paid_col

    # 사원번호 없으면 직원(이름)으로 나중에 daily의 미지정_N과 매칭
    if not emp_col:
        raise ValueError("휴가 엑셀에 '사원번호' 컬럼이 필요합니다.")
    if not type_col or type_col not in df.columns:
        raise ValueError("휴가 엑셀에 '휴가 유형' 컬럼이 필요합니다.")
    if not start_col or start_col not in df.columns:
        raise ValueError("휴가 엑셀에 '시작 시간' 컬럼이 필요합니다.")
    name_col = name_col if name_col and name_col in df.columns else None

    paid_rows = []
    highlight_rows = []
    unpaid_rows = []
    for _, r in df.iterrows():
        has_emp = pd.notna(r.get(emp_col)) and str(r.get(emp_col, "")).strip() != ""
        emp = _norm_employee_id(r[emp_col]) if has_emp else ""
        name = str(r.get(name_col, "") or "").strip() if name_col else ""
        if not has_emp and not name:
            continue
        try:
            d = pd.to_datetime(r[start_col]).date()
        except Exception:
            continue
        typ = str(r[type_col]).strip() if pd.notna(r[type_col]) else ""
        if paid_col:
            minutes = _parse_paid_hours(r[paid_col])
        else:
            minutes = 480

        # 연차(유급휴가)
        if "연차" in typ and "유급" in typ:
            paid_rows.append({"employee_id": emp, "employee_name": name, "date": d, "paid_leave_minutes": minutes})
            highlight_rows.append({"employee_id": emp, "employee_name": name, "date": d, "day_highlight": "paid_leave"})
        # 오프(무급휴가), 프리랜스 무급휴가
        elif "오프" in typ and "무급" in typ or "프리랜스" in typ and "무급" in typ:
            if unpaid_col:
                um = _parse_paid_hours(r[unpaid_col])
            else:
                um = 480
            if um <= 0:
                um = 480  # 값 없으면 1일(8h)로 간주
            unpaid_rows.append({"employee_id": emp, "employee_name": name, "date": d, "unpaid_leave_minutes": um})
            highlight_rows.append({"employee_id": emp, "employee_name": name, "date": d, "day_highlight": "unpaid_leave"})

    # 사원번호 없는 행은 employee_name으로 apply_leave_to_daily에서 daily의 미지정_N과 매칭 후 groupby
    paid_df = pd.DataFrame(paid_rows)
    if not paid_rows:
        paid_df = pd.DataFrame(columns=["employee_id", "employee_name", "date", "paid_leave_minutes"])
    else:
        paid_df = pd.DataFrame(paid_rows)

    highlight_df = pd.DataFrame(highlight_rows) if highlight_rows else pd.DataFrame(columns=["employee_id", "employee_name", "date", "day_highlight"])

    unpaid_df = pd.DataFrame(unpaid_rows)
    if not unpaid_rows:
        unpaid_df = pd.DataFrame(columns=["employee_id", "employee_name", "date", "unpaid_leave_minutes"])
    else:
        unpaid_df = pd.DataFrame(unpaid_rows)

    return paid_df, highlight_df, unpaid_df


def _finalize_day_highlight(daily: pd.DataFrame) -> None:
    """
    유급·무급 분이 모두 있으면 mixed_leave, 아니면 분에 맞게 paid/unpaid/normal.
    holiday_work(프리랜스 평일 공휴일 근무 등)는 덮어쓰지 않음.
    """
    if daily.empty or "day_highlight" not in daily.columns:
        return
    for i in daily.index:
        hl = str(daily.at[i, "day_highlight"] or "normal")
        if hl == "holiday_work":
            continue
        try:
            pl = int(float(daily.at[i, "paid_leave_minutes"] or 0))
            ul = int(float(daily.at[i, "unpaid_leave_minutes"] or 0))
        except (TypeError, ValueError):
            pl, ul = 0, 0
        if pl > 0 and ul > 0:
            daily.at[i, "day_highlight"] = "mixed_leave"
        elif ul > 0:
            daily.at[i, "day_highlight"] = "unpaid_leave"
        elif pl > 0:
            daily.at[i, "day_highlight"] = "paid_leave"
        elif hl in ("paid_leave", "unpaid_leave", "mixed_leave"):
            daily.at[i, "day_highlight"] = "normal"


def _inject_no_shifty_synthetic_rows(daily: pd.DataFrame) -> pd.DataFrame:
    """
    시프티 미등록 인원: daily 날짜 구간 내 평일·비(평일)공휴일마다 근무 행 추가.
    휴가 merge 전에 호출 → 유급/무급 휴가가 동일 사원·날짜에 합쳐질 수 있음.
    """
    spec_map = load_no_shifty_attendance()
    if not spec_map or daily.empty:
        return daily
    contract_types, employee_contracts = load_contract_config()
    try:
        dmin = _to_date(daily["date"].min())
        dmax = _to_date(daily["date"].max())
    except Exception:
        return daily
    holiday_dates = get_weekday_public_holidays_kr(dmin, dmax)
    existing = set(
        zip(daily["employee_id"].astype(str).map(_norm_employee_id), daily["date"].astype(str).str[:10])
    )
    new_rows: list[dict] = []
    for emp_key, spec in spec_map.items():
        emp = _norm_employee_id(emp_key)
        if not emp:
            continue
        meta = spec if isinstance(spec, dict) else {}
        name = str(meta.get("employee_name") or "").strip()
        try:
            net_m = int(meta.get("daily_net_minutes", 360))
        except (TypeError, ValueError):
            net_m = 360
        if net_m < 0:
            net_m = 0
        d = dmin
        while d <= dmax:
            if d.weekday() <= 4 and d not in holiday_dates:
                dkey = d.strftime("%Y-%m-%d")
                if (emp, dkey) not in existing:
                    ctype, scheduled = get_contract_for_employee(emp, d, contract_types, employee_contracts)
                    new_rows.append(
                        {
                            "employee_id": emp,
                            "employee_name": name,
                            "date": d,
                            "contract_type": ctype,
                            "scheduled_minutes": scheduled,
                            "work_minutes": net_m,
                            "break_minutes": 0,
                            "paid_leave_minutes": 0,
                            "unpaid_leave_minutes": 0,
                            "net_minutes": net_m,
                            "anomalies": [],
                        }
                    )
                    existing.add((emp, dkey))
            d += timedelta(days=1)
    if not new_rows:
        return daily
    extra = pd.DataFrame(new_rows)
    return pd.concat([daily, extra], ignore_index=True).sort_values(["employee_id", "date"]).reset_index(drop=True)


def apply_leave_to_daily(daily_path: Path, leave_path: Path | None, output_dir: Path) -> None:
    """daily_summary에 휴가(유급/무급) 반영 및 평일 공휴일 유급 반영 후 저장."""
    daily = pd.read_csv(daily_path, encoding="utf-8-sig")
    daily.columns = [str(c).strip().lstrip("\ufeff") for c in daily.columns]
    daily["date"] = pd.to_datetime(daily["date"]).dt.date
    daily["employee_id"] = daily["employee_id"].apply(_norm_employee_id)
    # attendance_normalizer 출력에는 없을 수 있음 — 하위 단계에서 기대하는 컬럼 미리 보장
    if "paid_leave_minutes" not in daily.columns:
        daily["paid_leave_minutes"] = 0
    if "unpaid_leave_minutes" not in daily.columns:
        daily["unpaid_leave_minutes"] = 0

    daily = _inject_no_shifty_synthetic_rows(daily)

    if leave_path is not None and leave_path.exists():
        paid_df, highlight_df, unpaid_df = load_leave_file(leave_path)
        # 사원번호 없는 인원: daily의 직원명 → 미지정_N 매칭
        name_to_emp_id = (
            daily.loc[daily["employee_id"].astype(str).str.startswith("미지정", na=False)]
            .groupby("employee_name")["employee_id"]
            .first()
            .to_dict()
        )
        def _resolve_id(df: pd.DataFrame) -> None:
            if "employee_name" not in df.columns:
                return
            mask = df["employee_id"].isna() | (df["employee_id"].astype(str).str.strip() == "")
            df.loc[mask, "employee_id"] = df.loc[mask, "employee_name"].map(lambda n: name_to_emp_id.get(str(n).strip(), ""))
            df.drop(columns=["employee_name"], inplace=True, errors="ignore")

        _resolve_id(paid_df)
        _resolve_id(highlight_df)
        _resolve_id(unpaid_df)
        del highlight_df  # 하이라이트는 분 단위로 _finalize_day_highlight에서 재산정
        if not paid_df.empty and "date" in paid_df.columns:
            paid_df = paid_df.groupby(["employee_id", "date"], as_index=False)["paid_leave_minutes"].sum()
        if not unpaid_df.empty and "date" in unpaid_df.columns:
            unpaid_df = unpaid_df.groupby(["employee_id", "date"], as_index=False)["unpaid_leave_minutes"].sum()
    else:
        paid_df = pd.DataFrame(columns=["employee_id", "date", "paid_leave_minutes"])
        highlight_df = pd.DataFrame(columns=["employee_id", "date", "day_highlight"])
        unpaid_df = pd.DataFrame(columns=["employee_id", "date", "unpaid_leave_minutes"])

    # 날짜를 YYYY-MM-DD 문자열로 통일해 merge
    daily["_date_key"] = daily["date"].astype(str).str[:10]
    if not paid_df.empty:
        paid_df = paid_df.copy()
        paid_df["employee_id"] = paid_df["employee_id"].apply(_norm_employee_id)
        paid_df["_date_key"] = paid_df["date"].astype(str).str[:10]
        paid_merge = paid_df.groupby(["employee_id", "_date_key"], as_index=False)["paid_leave_minutes"].sum()
        paid_merge = paid_merge.rename(columns={"paid_leave_minutes": "leave_min"})
        daily = daily.merge(paid_merge, on=["employee_id", "_date_key"], how="left")
        daily["leave_min"] = daily["leave_min"].fillna(0).astype(int)
        daily["paid_leave_minutes"] = daily["paid_leave_minutes"].fillna(0).astype(int) + daily["leave_min"]
        daily = daily.drop(columns=["leave_min", "_date_key"])
    if not unpaid_df.empty:
        if "unpaid_leave_minutes" not in daily.columns:
            daily["unpaid_leave_minutes"] = 0
        daily["_date_key"] = daily["date"].astype(str).str[:10]
        unpaid_df = unpaid_df.copy()
        unpaid_df["employee_id"] = unpaid_df["employee_id"].apply(_norm_employee_id)
        unpaid_df["_date_key"] = unpaid_df["date"].astype(str).str[:10]
        unpaid_merge = unpaid_df.groupby(["employee_id", "_date_key"], as_index=False)["unpaid_leave_minutes"].sum()
        unpaid_merge = unpaid_merge.rename(columns={"unpaid_leave_minutes": "unpaid_merge_min"})
        daily = daily.merge(unpaid_merge, on=["employee_id", "_date_key"], how="left")
        daily["unpaid_leave_minutes"] = (daily["unpaid_leave_minutes"].fillna(0).astype(int) + daily["unpaid_merge_min"].fillna(0).astype(int))
        daily = daily.drop(columns=["unpaid_merge_min", "_date_key"], errors="ignore")
    else:
        daily["unpaid_leave_minutes"] = 0
        daily = daily.drop(columns=["_date_key"], errors="ignore")

    # net_minutes = 실근무 − 휴게 + 유급휴가 → payroll 셀에 합산 시간 노출
    daily["net_minutes"] = daily["work_minutes"] - daily["break_minutes"] + daily["paid_leave_minutes"]

    # 휴가만 있고 출퇴근 기록이 없는 날도 payroll에 나오도록 daily에 행 추가
    # 유급만 먼저 넣고 무급을 나중에 넣으면 existing 때문에 무급이 누락되므로, (사원,일)당 1행으로 유급·무급 동시 반영
    contract_types, employee_contracts = load_contract_config()
    existing = set(zip(daily["employee_id"].astype(str), daily["date"].astype(str).str[:10]))
    leave_only = []

    def _ctype_sched(emp, dkey):
        ctype, scheduled = get_contract_for_employee(emp, dkey, contract_types, employee_contracts)
        return ctype, scheduled

    paid_by_key = {}
    if not paid_df.empty:
        ptmp = paid_df.copy()
        if "_date_key" not in ptmp.columns:
            ptmp["_date_key"] = ptmp["date"].astype(str).str[:10]
        ptmp["employee_id"] = ptmp["employee_id"].apply(_norm_employee_id)
        for (emp, dkey), grp in ptmp.groupby(["employee_id", "_date_key"]):
            paid_by_key[(str(emp), dkey)] = int(grp["paid_leave_minutes"].sum())

    unpaid_by_key = {}
    if not unpaid_df.empty:
        utmp = unpaid_df.copy()
        if "_date_key" not in utmp.columns:
            utmp["_date_key"] = utmp["date"].astype(str).str[:10]
        utmp["employee_id"] = utmp["employee_id"].apply(_norm_employee_id)
        for (emp, dkey), grp in utmp.groupby(["employee_id", "_date_key"]):
            unpaid_by_key[(str(emp), dkey)] = int(grp["unpaid_leave_minutes"].sum())

    for (emp, dkey) in sorted(set(paid_by_key.keys()) | set(unpaid_by_key.keys())):
        if (str(emp), dkey) in existing:
            continue
        pl = paid_by_key.get((emp, dkey), 0)
        ul = unpaid_by_key.get((emp, dkey), 0)
        if pl <= 0 and ul <= 0:
            continue

        name = ""
        em = daily["employee_id"].apply(_norm_employee_id)
        if (em == str(emp)).any():
            name = daily.loc[em == str(emp), "employee_name"].iloc[0]
        ctype, scheduled = _ctype_sched(emp, dkey)
        cap_min = min(pl, scheduled) if scheduled and scheduled > 0 and pl > 0 else pl

        if pl > 0 and ul > 0:
            leave_only.append({
                "employee_id": emp, "employee_name": name, "date": pd.to_datetime(dkey).date(),
                "contract_type": ctype, "scheduled_minutes": scheduled,
                "work_minutes": 0, "break_minutes": 0,
                "paid_leave_minutes": cap_min,
                "unpaid_leave_minutes": ul,
                "net_minutes": cap_min,
                "anomalies": [], "day_highlight": "mixed_leave",
            })
        elif pl > 0:
            leave_only.append({
                "employee_id": emp, "employee_name": name, "date": pd.to_datetime(dkey).date(),
                "contract_type": ctype, "scheduled_minutes": scheduled,
                "work_minutes": 0, "break_minutes": 0,
                "paid_leave_minutes": cap_min,
                "unpaid_leave_minutes": 0,
                "net_minutes": cap_min,
                "anomalies": [], "day_highlight": "paid_leave",
            })
        else:
            um = ul
            if um < scheduled and scheduled > 0:
                work_min = scheduled - um
                net_min = work_min
            else:
                work_min = 0
                net_min = 0
            leave_only.append({
                "employee_id": emp, "employee_name": name, "date": pd.to_datetime(dkey).date(),
                "contract_type": ctype, "scheduled_minutes": scheduled,
                "work_minutes": work_min, "break_minutes": 0,
                "paid_leave_minutes": 0,
                "unpaid_leave_minutes": um,
                "net_minutes": net_min,
                "anomalies": [], "day_highlight": "unpaid_leave",
            })
        existing.add((str(emp), dkey))

    if leave_only:
        daily = pd.concat([daily, pd.DataFrame(leave_only)], ignore_index=True)
        daily = daily.sort_values(["employee_id", "date"]).reset_index(drop=True)
        daily["net_minutes"] = daily["work_minutes"] - daily["break_minutes"] + daily["paid_leave_minutes"]

    # 평일 공휴일: 유급휴일로 계약별 scheduled_minutes 지급. 당일 근무한 경우 휴일 지급 + 실제 근무(야근 반영)
    date_min = _to_date(daily["date"].min())
    date_max = _to_date(daily["date"].max())
    holiday_dates = get_weekday_public_holidays_kr(date_min, date_max)
    if holiday_dates:
        daily["_dkey"] = daily["date"].apply(_date_to_dkey)
        existing = set(zip(daily["employee_id"].astype(str), daily["_dkey"].astype(str)))
        emp_names = daily.groupby("employee_id")["employee_name"].first().to_dict()
        holiday_only = []
        for emp in daily["employee_id"].unique():
            for hdate in holiday_dates:
                dkey = hdate.strftime("%Y-%m-%d")
                if _is_freelancer(emp, employee_contracts):
                    # 프리랜스: 유급휴일 없음. 근무한 날은 holiday_work 표시 → 야근 1.5배만, 미근무일은 0시간
                    if (str(emp), dkey) in existing:
                        mask = (daily["employee_id"].astype(str) == str(emp)) & (daily["_dkey"] == dkey)
                        idx = daily.index[mask]
                        if len(idx) > 0:
                            daily.loc[idx, "day_highlight"] = "holiday_work"
                    else:
                        name = emp_names.get(emp, "")
                        ctype, _ = _ctype_sched(emp, dkey)
                        holiday_only.append({
                            "employee_id": emp, "employee_name": name, "date": hdate,
                            "contract_type": ctype, "scheduled_minutes": 0,
                            "work_minutes": 0, "break_minutes": 0, "paid_leave_minutes": 0,
                            "unpaid_leave_minutes": 0,
                            "net_minutes": 0, "anomalies": [], "day_highlight": "normal",
                        })
                        existing.add((str(emp), dkey))
                    continue
                if (str(emp), dkey) in existing:
                    mask = (daily["employee_id"].astype(str) == str(emp)) & (daily["_dkey"] == dkey)
                    ctype, scheduled = _ctype_sched(emp, dkey)
                    idx = daily.index[mask]
                    if len(idx) > 0:
                        daily.loc[idx, "paid_leave_minutes"] = (
                            daily.loc[idx, "paid_leave_minutes"].astype(int) + scheduled
                        )
                else:
                    ctype, scheduled = _ctype_sched(emp, dkey)
                    if scheduled <= 0:
                        continue
                    name = emp_names.get(emp, "")
                    holiday_only.append({
                        "employee_id": emp, "employee_name": name, "date": hdate,
                        "contract_type": ctype, "scheduled_minutes": scheduled,
                        "work_minutes": 0, "break_minutes": 0, "paid_leave_minutes": scheduled,
                        "unpaid_leave_minutes": 0,
                        "net_minutes": scheduled, "anomalies": [], "day_highlight": "paid_leave",
                    })
                    existing.add((str(emp), dkey))
        if holiday_only:
            daily = pd.concat([daily, pd.DataFrame(holiday_only)], ignore_index=True)
            daily = daily.sort_values(["employee_id", "date"]).reset_index(drop=True)
        daily["net_minutes"] = daily["work_minutes"] - daily["break_minutes"] + daily["paid_leave_minutes"]
        daily = daily.drop(columns=["_dkey"], errors="ignore")

    # 하위 단계(payroll_calculator 등)에서 기대하는 컬럼 보장
    if "unpaid_leave_minutes" not in daily.columns:
        daily["unpaid_leave_minutes"] = 0
    if "day_highlight" not in daily.columns:
        daily["day_highlight"] = "normal"
    else:
        daily["day_highlight"] = daily["day_highlight"].fillna("normal")

    _finalize_day_highlight(daily)

    daily.to_csv(output_dir / "daily_summary.csv", index=False, encoding="utf-8-sig")


def main(daily_path: Path, leave_path: Path, output_dir: Path) -> None:
    apply_leave_to_daily(daily_path, leave_path, output_dir)
    print("✅ 휴가 반영 완료 (daily_summary 갱신)")


if __name__ == "__main__":
    out = Path("output")
    main(out / "daily_summary.csv", Path("input_leave.xlsx"), out)
