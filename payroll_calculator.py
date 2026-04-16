import pandas as pd
from pathlib import Path
from datetime import date, timedelta

# 스크립트 위치 기준 output (어디서 실행해도 동일)
ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "output"

HOURLY_WAGE = 11000
OT_MULTIPLIER = 1.5
DAILY_OT_THRESHOLD = 8 * 60


def _infer_payroll_period(daily: pd.DataFrame) -> tuple[date, date]:
    """
    급여산정기간: 전월 25일 ~ 당월 24일.
    daily 날짜 범위에서 추정. max_date 기준으로 period end = 당월 24일.
    """
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
        # max가 25일 이후면 다음 달 급여
        start = date(y, m, 25)
        if m == 12:
            end = date(y + 1, 1, 24)
        else:
            end = date(y, m + 1, 24)
    return start, end


def _week_sunday(week_start) -> date:
    """주 시작(월) + 6일 = 일요일."""
    d = pd.to_datetime(week_start)
    return (d + pd.Timedelta(days=6)).date()


# 계약 기준 프리랜스 판별용 캐시
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


def _row_date(row) -> date | None:
    """행의 date 컬럼을 date 객체로. holiday_dates(set of date)와 비교 가능하도록 항상 date 반환."""
    d = row.get("date") if hasattr(row, "get") else getattr(row, "date", None)
    if d is None or (isinstance(d, float) and d != d):
        return None
    if hasattr(d, "date") and callable(getattr(d, "date")):
        d = d.date()
    if isinstance(d, date) and not hasattr(d, "hour"):
        return d
    return pd.to_datetime(d).date()


def calc_daily(row, holiday_dates: set | None = None):
    """
    일별 기본급·야근 계산.
    프리랜스가 평일 공휴일(명절연휴 등)에 근무한 경우 전부 야근 1.5배 (day_highlight 또는 holiday_dates로 판단).
    """
    emp = row.get("employee_id", "") if hasattr(row, "get") else getattr(row, "employee_id", "")
    is_freelancer = _is_freelancer(emp)
    day_hl = row.get("day_highlight", None) if hasattr(row, "get") else getattr(row, "day_highlight", None)
    row_d = _row_date(row)
    # 프리랜스 + (day_highlight가 holiday_work 이거나, 해당 일자가 공휴일 목록에 있음) → 전부 야근
    is_holiday_work = (day_hl == "holiday_work") or (
        bool(holiday_dates) and row_d is not None and row_d in holiday_dates
    )
    if is_freelancer and is_holiday_work:
        base_min = 0
        ot_min = int(row.get("net_minutes", 0) if hasattr(row, "get") else row.net_minutes)
    else:
        net = int(row.get("net_minutes", 0) if hasattr(row, "get") else row.net_minutes)
        base_min = min(net, DAILY_OT_THRESHOLD)
        ot_min = max(net - DAILY_OT_THRESHOLD, 0)

    base_pay = base_min / 60 * HOURLY_WAGE
    ot_pay = ot_min / 60 * HOURLY_WAGE * OT_MULTIPLIER

    return base_pay, ot_pay


def main(output_dir=None):
    out = Path(output_dir) if output_dir is not None else OUTPUT_DIR
    daily = pd.read_csv(out / "daily_summary.csv", encoding="utf-8-sig")
    daily.columns = [str(c).strip().lstrip("\ufeff") for c in daily.columns]
    weekly = pd.read_csv(out / "weekly_allowance_result.csv")

    if "unpaid_leave_minutes" not in daily.columns:
        daily["unpaid_leave_minutes"] = 0

    daily["date"] = pd.to_datetime(daily["date"]).dt.normalize()
    payroll_start, payroll_end = _infer_payroll_period(daily)
    holiday_dates = set()
    try:
        from leave_merger import get_weekday_public_holidays_kr
        holiday_dates = get_weekday_public_holidays_kr(payroll_start, payroll_end)
    except Exception:
        pass
    period_dates_set = set(
        (payroll_start + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((payroll_end - payroll_start).days + 1)
    )

    all_dates = sorted(daily["date"].dt.strftime("%Y-%m-%d").unique())
    employee_name_col = "employee_name" if "employee_name" in daily.columns else None

    # 주휴일=일요일. 주휴수당은 해당 주 일요일이 산정기간 내에 있을 때만 이번 급여에 포함
    week_starts = sorted(weekly["week_start"].unique())
    week_to_col = {ws: f"주휴{i+1}" for i, ws in enumerate(week_starts)}
    weeks_in_period = {
        ws for ws in week_starts
        if payroll_start <= _week_sunday(ws) <= payroll_end
    }
    payroll_start_str = payroll_start.strftime("%Y-%m-%d")
    date_to_header = lambda d: f"{pd.to_datetime(d).month}/{pd.to_datetime(d).day}"

    def header_for_date(d):
        """날짜가 산정기간 이전이면 (주휴용) 두번째 줄에 표시."""
        h = date_to_header(d)
        return f"{h}\n주휴용" if d < payroll_start_str else h

    def date_week_start(d):
        dt = pd.to_datetime(d)
        return (dt - pd.to_timedelta(dt.weekday(), unit="D")).strftime("%Y-%m-%d")

    # 모든 날짜 컬럼 포함(주휴 계산용 앞일자 포함). 주휴는 산정기간 내 일요일인 주만
    cols_order = []
    if week_starts:
        for ws in week_starts:
            ws_str = pd.to_datetime(ws).strftime("%Y-%m-%d")
            week_dates = [
                d for d in all_dates
                if date_week_start(d) == ws_str
                and pd.to_datetime(d).weekday() <= 4
            ]
            for d in sorted(week_dates):
                cols_order.append((d, header_for_date(d)))
            if ws in weeks_in_period:
                cols_order.append((week_to_col[ws], week_to_col[ws]))
        seen = set(k for k, _ in cols_order if len(k) == 10)
        for d in all_dates:
            if d not in seen and pd.to_datetime(d).weekday() <= 4:
                cols_order.append((d, header_for_date(d)))
    else:
        for d in all_dates:
            if pd.to_datetime(d).weekday() <= 4:
                cols_order.append((d, header_for_date(d)))
    pay_cols = ["base_pay", "overtime_pay", "overtime_hours", "weekly_allowance_pay", "weekly_allowance_hours", "unpaid_hours", "total_pay"]
    col_headers = [h for _, h in cols_order]
    col_to_week = {c: pd.to_datetime(ws).strftime("%Y-%m-%d") for ws, c in week_to_col.items()}

    rows = []

    for emp, g in daily.groupby("employee_id"):
        base_total = 0
        ot_total = 0
        day_net = g.set_index("date")["net_minutes"]
        g_period = g[g["date"].dt.strftime("%Y-%m-%d").isin(period_dates_set)]
        unpaid_total = int(g_period["unpaid_leave_minutes"].sum())

        for _, r in g_period.iterrows():
            b, o = calc_daily(r, holiday_dates=holiday_dates)
            base_total += b
            ot_total += o

        def _week_in_period(ws):
            try:
                sun = _week_sunday(ws)
                return payroll_start <= sun <= payroll_end
            except Exception:
                return False

        wk = weekly[(weekly.employee_id == emp) & weekly["week_start"].apply(_week_in_period)]
        allow_min = wk["weekly_allowance_minutes"].sum()
        # 표시 시간(소수 1자리)과 금액 일치: 시간 = round(분/60, 1), 금액 = round(시간 × 시급)
        allow_hrs = round(allow_min / 60, 1)
        allow_pay = round(allow_hrs * HOURLY_WAGE, 0)

        name = g[employee_name_col].iloc[0] if employee_name_col else ""
        first_date = g_period["date"].min() if not g_period.empty else None
        first_attendance_date = first_date.strftime("%Y-%m-%d") if first_date is not None and pd.notna(first_date) else ""
        row = {"employee_id": emp, "employee_name": name, "first_attendance_date": first_attendance_date}
        wk_ws = pd.to_datetime(wk["week_start"]).dt.strftime("%Y-%m-%d")

        for k, h in cols_order:
            if len(k) == 10:  # date key
                match = day_net.index[day_net.index.astype(str).str[:10] == k]
                if len(match):
                    row[h] = round(day_net[match].iloc[0] / 60, 1)
                else:
                    row[h] = ""
            else:  # 주휴N
                ws_str = col_to_week.get(k, "")
                m = wk[wk_ws == ws_str] if ws_str else pd.DataFrame()
                row[h] = round(m["weekly_allowance_minutes"].iloc[0] / 60, 1) if len(m) else 0

        total = base_total + ot_total + allow_pay
        row["base_pay"] = round(base_total, 0)
        row["overtime_pay"] = round(ot_total, 0)
        row["overtime_hours"] = round(ot_total / (HOURLY_WAGE * OT_MULTIPLIER), 1)
        row["weekly_allowance_pay"] = int(allow_pay)
        row["weekly_allowance_hours"] = allow_hrs
        row["unpaid_hours"] = round(unpaid_total / 60, 1) if unpaid_total else 0
        row["total_pay"] = round(total, 0)
        rows.append(row)

    df = pd.DataFrame(rows)
    cols = ["employee_id", "employee_name", "first_attendance_date"] + [h for _, h in cols_order] + pay_cols
    df = df[cols]
    out.mkdir(parents=True, exist_ok=True)
    df.to_csv(out / "payroll_result.csv", index=False, encoding="utf-8-sig")

    print("✅ payroll_result.csv 생성")


if __name__ == "__main__":
    main()
