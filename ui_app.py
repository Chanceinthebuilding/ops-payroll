import streamlit as st
import pandas as pd
from pathlib import Path

# 스크립트 위치 기준 output (어디서 실행해도 동일)
OUTPUT_DIR = Path(__file__).resolve().parent / "output"

st.title("HR 급여 검증 MVP")

daily_path = OUTPUT_DIR / "daily_summary.csv"
payroll_path = OUTPUT_DIR / "payroll_result.csv"
if not daily_path.exists() or not payroll_path.exists():
    st.warning("출력 데이터가 없습니다. 먼저 터미널에서 **`python run_all.py`** 를 실행한 뒤 새로고침하세요.")
    st.code("python run_all.py", language="text")
    st.stop()

daily = pd.read_csv(daily_path)
payroll = pd.read_csv(payroll_path)
anomaly = pd.read_csv(OUTPUT_DIR / "anomaly_report.csv") if (OUTPUT_DIR / "anomaly_report.csv").exists() else pd.DataFrame()

emp = st.selectbox("직원 선택", daily.employee_id.unique())

st.subheader("일별 근무")
st.dataframe(daily[daily.employee_id == emp])

st.subheader("급여 결과")
st.dataframe(payroll[payroll.employee_id == emp])

st.subheader("이상치")
if not anomaly.empty and "employee_id" in anomaly.columns:
    st.dataframe(anomaly[anomaly.employee_id == emp])
else:
    st.caption("이상치 없음")
