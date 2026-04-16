import pandas as pd
from pathlib import Path

OUTPUT_DIR = Path(__file__).resolve().parent / "output"


def build_anomaly_report(daily_df):
    rows = []

    for _, r in daily_df.iterrows():
        if r.anomalies and len(eval(r.anomalies)) > 0:
            rows.append({
                "employee_id": r.employee_id,
                "date": r.date,
                "anomalies": r.anomalies,
                "net_minutes": r.net_minutes
            })

    return pd.DataFrame(rows)


def main(output_dir=None):
    out = Path(output_dir) if output_dir is not None else OUTPUT_DIR
    df = pd.read_csv(out / "daily_summary.csv")
    rep = build_anomaly_report(df)
    out.mkdir(parents=True, exist_ok=True)
    rep.to_csv(out / "anomaly_report.csv", index=False, encoding="utf-8-sig")
    print("✅ anomaly_report.csv 생성")


if __name__ == "__main__":
    main()
