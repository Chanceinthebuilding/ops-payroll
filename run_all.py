"""
One-stop 실행: 출퇴근 정규화 → 주간 수당 → 급여 계산 → 이상치 리포트를 순서대로 실행합니다.
프로젝트 루트에서 실행: python run_all.py
웹 업로드 시: run_pipeline(input_path, output_dir) 로 호출
"""
from pathlib import Path

# 스크립트가 있는 디렉터리를 작업 디렉터리로 고정 (어디서 실행해도 동일 동작)
ROOT = Path(__file__).resolve().parent


def run_pipeline(
    input_path: Path | None = None,
    output_dir: Path | None = None,
    leave_path: Path | None = None,
):
    """
    업로드된 엑셀 경로와 결과 디렉터리를 받아 파이프라인 실행.
    input_path=None 이면 기존처럼 input/ 폴더에서 xlsx 1개 사용.
    output_dir=None 이면 프로젝트의 output/ 사용.
    leave_path=휴가 엑셀 경로 이면 출퇴근 처리 후 유급 휴가를 반영합니다.
    """
    import attendance_normalizer
    import rule_engine
    import payroll_calculator
    import anomaly_reporter

    out = output_dir if output_dir is not None else ROOT / "output"
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)

    attendance_normalizer.main(input_path=input_path, output_dir=out)
    import leave_merger
    leave_merger.apply_leave_to_daily(out / "daily_summary.csv", Path(leave_path) if leave_path else None, out)
    rule_engine.main(output_dir=out)
    payroll_calculator.main(output_dir=out)
    anomaly_reporter.main(output_dir=out)


def main():
    import os
    os.chdir(ROOT)

    print("=== 1. 출퇴근 정규화 ===")
    run_pipeline()

    print("\n✅ 전체 파이프라인 완료. output/ 폴더를 확인하세요.")


if __name__ == "__main__":
    main()
