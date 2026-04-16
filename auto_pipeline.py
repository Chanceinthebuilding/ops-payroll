"""
인건비 자동화 파이프라인

흐름:
  1. Shiftee → 출퇴근기록 xlsx 다운로드
  2. Shiftee → 휴가내역 xlsx 다운로드
  3. run_pipeline() → 급여 계산 (output/ 저장)
  4. google_sheet_exporter → 구글 시트 업데이트

실행:
  python auto_pipeline.py           # 전체 (다운로드 + 계산 + 구글시트)
  python auto_pipeline.py --skip-download  # 다운로드 생략 (input/ 파일 재사용)
  python auto_pipeline.py --skip-sheets    # 구글시트 업데이트 생략
"""
import argparse
import asyncio
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
INPUT_DIR = ROOT / "input"
OUTPUT_DIR = ROOT / "output"


# ──────────────────────────────────────────────
# 파일 선택
# ──────────────────────────────────────────────

def find_latest(prefix: str) -> Path | None:
    """input/ 에서 prefix로 시작하는 가장 최근 xlsx 반환."""
    files = [
        f for f in INPUT_DIR.glob(f"{prefix}*.xlsx")
        if not f.name.startswith("~$")
    ]
    return max(files, key=lambda f: f.stat().st_mtime) if files else None


# ──────────────────────────────────────────────
# 단계별 실행
# ──────────────────────────────────────────────

async def step_download() -> dict[str, Path]:
    """1·2단계: Shiftee에서 출퇴근 + 휴가 xlsx 다운로드."""
    from shiftee_downloader import run as shiftee_run
    print("\n[1/3] Shiftee 다운로드")
    result = await shiftee_run(mode="all")
    return result


def step_pipeline(attendance_path: Path, leave_path: Path | None) -> Path:
    """3단계: 급여 파이프라인 실행 → output/ 저장."""
    print("\n[2/3] 급여 파이프라인 실행")
    print(f"  출퇴근: {attendance_path.name}")
    if leave_path:
        print(f"  휴가:   {leave_path.name}")
    else:
        print("  휴가:   없음 (생략)")

    from run_all import run_pipeline
    OUTPUT_DIR.mkdir(exist_ok=True)
    run_pipeline(
        input_path=attendance_path,
        output_dir=OUTPUT_DIR,
        leave_path=leave_path,
    )
    print(f"  ✅ 결과 저장: {OUTPUT_DIR}")
    return OUTPUT_DIR


def step_google_sheet(output_dir: Path) -> str:
    """4단계: 구글 시트 업데이트."""
    print("\n[3/3] 구글 시트 업데이트")
    from google_sheet_exporter import create_google_sheet
    url = create_google_sheet(output_dir)
    print(f"  ✅ 시트 URL: {url}")
    return url


# ──────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="인건비 자동화 파이프라인")
    parser.add_argument("--skip-download", action="store_true", help="Shiftee 다운로드 생략 (input/ 재사용)")
    parser.add_argument("--skip-sheets",   action="store_true", help="구글 시트 업데이트 생략")
    args = parser.parse_args()

    started_at = datetime.now()
    print(f"=== auto_pipeline 시작: {started_at.strftime('%Y-%m-%d %H:%M:%S')} ===")

    try:
        # 1·2단계: 다운로드
        if args.skip_download:
            print("\n[1/3] 다운로드 생략 — input/ 폴더 파일 사용")
            attendance_path = find_latest("SHIFTEE-ATTENDANCES")
            leave_path      = find_latest("SHIFTEE-LEAVES")
            if not attendance_path:
                print("❌ input/ 에 출퇴근 xlsx가 없습니다.")
                sys.exit(1)
            print(f"  출퇴근: {attendance_path.name}")
            print(f"  휴가:   {leave_path.name if leave_path else '없음'}")
        else:
            result = asyncio.run(step_download())
            attendance_path = result.get("attendance")
            leave_path      = result.get("leaves")
            if not attendance_path:
                print("❌ 출퇴근 파일 다운로드 실패")
                sys.exit(1)

        # 3단계: 파이프라인
        output_dir = step_pipeline(attendance_path, leave_path)

        # 4단계: 구글 시트
        if args.skip_sheets:
            print("\n[3/3] 구글 시트 생략")
        else:
            step_google_sheet(output_dir)

        elapsed = (datetime.now() - started_at).seconds
        print(f"\n=== 완료 ({elapsed}초) ===")

    except KeyboardInterrupt:
        print("\n중단됨.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 오류: {e}")
        raise


if __name__ == "__main__":
    main()
