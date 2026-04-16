"""
주휴수당 규칙: 주휴일 일요일. 월~금 기준으로 계산.
- 일 최대 반영 8시간(480분) → 그날 net_minutes를 480으로 캡한 뒤, (캡한 합) / 5 = 주휴시간.
- 주휴시간은 최대 8시간(480분)으로 한정.
- 오프(무급)가 같은 주에 2일 이상이고, 그 2일 모두 근무 0h이면 해당 주 주휴수당 미지급.
- 급여산정기간 내 0시간 오프(무급휴가): 최초 발생 주간에만 주휴수당 인정, 이후 주간에 0시간 오프가 있으면 해당 주 주휴수당 0.
- 프리랜스: 한 주 근무시간 총합 15시간 미만이면 해당 주 주휴수당 0.
- 프리랜스: 명절연휴 등 평일 공휴일(2/16~2/18 등)에는 주휴 산정에 0시간 반영. 해당일 근무는 야근만(이미 daily에서 holiday_work 처리).
  주휴시간 = (공휴일 제외한 평일 근무 합) / 5. 예: 2/19 8h + 2/20 8h → 16/5 = 3.2시간.
"""
import pandas as pd
from pathlib import Path
from datetime import date

ROOT = Path(__file__).resolve().parent

try:
    from leave_merger import get_weekday_public_holidays_kr
except ImportError:
    get_weekday_public_holidays_kr = None
MAX_DAILY_MINUTES = 8 * 60  # 480분 = 일 최대 반영
MAX_WEEKLY_ALLOWANCE_MINUTES = 8 * 60  # 480분 = 주휴 최대 8시간
MIN_WEEKLY_MINUTES_FREELANCER = 15 * 60  # 900분 = 프리랜스 주휴 지급 최소 주간 근무 15시간

_employee_contracts_cache = None


def _is_freelancer(emp) -> bool:
    """계약이 freelancer_*이거나 사번이 F로 시작하면 프리랜스."""
    global _employee_contracts_cache
    if _employee_contracts_cache is None:
        try:
            from attendance_normalizer import load_contract_config
            _, _employee_contracts_cache = load_contract_config()
        except Exception:
            _employee_contracts_cache = {}
    key = str(emp).strip()
    try:
        if isinstance(emp, (int, float)) and str(emp) != "nan":
            key = str(int(emp))
    except (ValueError, TypeError):
        pass
    ctype = (_employee_contracts_cache or {}).get(key) or (_employee_contracts_cache or {}).get("default")
    if ctype and str(ctype).startswith("freelancer_"):
        return True
    return str(emp).strip().upper().startswith("F")


def _infer_payroll_period(daily: pd.DataFrame) -> tuple[date, date]:
    """급여산정기간: 전월 25일 ~ 당월 24일. daily 날짜 범위에서 추정."""
    daily = daily.copy()
    daily["date"] = pd.to_datetime(daily["date"]).dt.normalize()
    max_d = daily["date"].max()
    if pd.isna(max_d):
        from datetime import datetime
        now = datetime.now()
        y, m = now.year, now.month
        end = date(y, m, 24)
        start = date(y, m - 1, 25) if m > 1 else date(y - 1, 12, 25)
        return start, end
    max_d = max_d.date() if hasattr(max_d, "date") else max_d
    y, m, d = max_d.year, max_d.month, max_d.day
    if d <= 24:
        end = date(y, m, 24)
        start = date(y, m - 1, 25) if m > 1 else date(y - 1, 12, 25)
    else:
        start = date(y, m, 25)
        end = date(y + 1, 1, 24) if m == 12 else date(y, m + 1, 24)
    return start, end


def _week_sunday(week_start) -> date:
    """주 시작(월) + 6일 = 일요일."""
    d = pd.to_datetime(week_start)
    return (d + pd.Timedelta(days=6)).date()


# -------------------------
# 주간 집계 (주휴일 = 일요일 → 주 단위는 월요일 시작)
# -------------------------
def build_weekly_allowance(daily):
    daily = daily.copy()
    daily["date"] = pd.to_datetime(daily["date"]).dt.normalize()
    daily["weekday"] = daily["date"].dt.weekday  # 월=0, 일=6
    weekdays_df = daily[daily["weekday"] <= 4].copy()
    weekdays_df["week_start"] = weekdays_df["date"] - pd.to_timedelta(weekdays_df["weekday"], unit="D")

    if "day_highlight" not in weekdays_df.columns:
        weekdays_df["day_highlight"] = "normal"

    payroll_start, payroll_end = _infer_payroll_period(daily)
    # 프리랜스 주휴 산정 시 제외할 평일 공휴일 (명절연휴 등)
    holiday_dates = set()
    if get_weekday_public_holidays_kr is not None:
        try:
            date_min = daily["date"].min()
            date_max = daily["date"].max()
            if pd.notna(date_min) and pd.notna(date_max):
                d_min = date_min.date() if hasattr(date_min, "date") else pd.to_datetime(date_min).date()
                d_max = date_max.date() if hasattr(date_max, "date") else pd.to_datetime(date_max).date()
                holiday_dates = get_weekday_public_holidays_kr(d_min, d_max)
        except Exception:
            pass

    # 급여산정기간 내, 직원별로 0시간 오프가 최초로 발생한 주(week_start)만 수집 (시간순 첫 주)
    emp_weeks_with_zero_off = []
    for (emp, wk), g in weekdays_df.groupby(["employee_id", "week_start"]):
        sun = _week_sunday(wk)
        if not (payroll_start <= sun <= payroll_end):
            continue
        unpaid_zero = ((g["day_highlight"] == "unpaid_leave") & (g["net_minutes"] == 0)).sum()
        if unpaid_zero >= 1:
            w_start = wk.date() if hasattr(wk, "date") else pd.to_datetime(wk).date()
            emp_weeks_with_zero_off.append((emp, w_start))
    first_zero_off_week_by_emp = {}
    for (emp, w_start) in sorted(emp_weeks_with_zero_off, key=lambda x: (x[0], x[1])):
        if emp not in first_zero_off_week_by_emp:
            first_zero_off_week_by_emp[emp] = w_start

    rows = []
    for (emp, wk), g in weekdays_df.groupby(["employee_id", "week_start"]):
        # 프리랜스: 평일 공휴일(명절연휴 등)은 주휴 산정에서 제외. 해당일 근무는 야근만 반영(이미 holiday_work).
        if _is_freelancer(emp) and holiday_dates:
            g_dates = g["date"].dt.date
            g_non_holiday = g[~g_dates.isin(holiday_dates)]
            capped_per_day = g_non_holiday["net_minutes"].clip(upper=MAX_DAILY_MINUTES)
            capped_total = int(capped_per_day.sum())
        else:
            capped_per_day = g["net_minutes"].clip(upper=MAX_DAILY_MINUTES)
            capped_total = int(capped_per_day.sum())
        unpaid_zero = ((g["day_highlight"] == "unpaid_leave") & (g["net_minutes"] == 0)).sum()

        if unpaid_zero >= 2:
            allow_min = 0
        else:
            allow_min = min(round(capped_total / 5), MAX_WEEKLY_ALLOWANCE_MINUTES)

        # 프리랜스: 한 주 근무시간 총합 15시간 미만이면 주휴수당 0
        if _is_freelancer(emp) and capped_total < MIN_WEEKLY_MINUTES_FREELANCER:
            allow_min = 0

        # 급여산정기간 내 0시간 오프: 최초 발생 주만 인정, 이후 주는 0
        wk_date = wk.date() if hasattr(wk, "date") else pd.to_datetime(wk).date()
        sun = _week_sunday(wk)
        if payroll_start <= sun <= payroll_end and unpaid_zero >= 1:
            first_week = first_zero_off_week_by_emp.get(emp)
            if first_week is not None and wk_date != first_week:
                allow_min = 0

        rows.append({
            "employee_id": emp,
            "week_start": wk_date,
            "total_work_minutes": int(g["net_minutes"].sum()),
            "weekly_allowance_minutes": allow_min,
        })

    return pd.DataFrame(rows)


def main(output_dir=None):
    out = Path(output_dir) if output_dir is not None else Path("output")
    daily = pd.read_csv(out / "daily_summary.csv", encoding="utf-8-sig")
    daily.columns = [str(c).strip().lstrip("\ufeff") for c in daily.columns]
    if "day_highlight" not in daily.columns:
        daily["day_highlight"] = "normal"
    if "unpaid_leave_minutes" not in daily.columns:
        daily["unpaid_leave_minutes"] = 0
    result = build_weekly_allowance(daily)
    result.to_csv(out / "weekly_allowance_result.csv", index=False, encoding="utf-8-sig")
    print("✅ weekly_allowance_result.csv 생성")


if __name__ == "__main__":
    main()
