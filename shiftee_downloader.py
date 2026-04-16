"""
Shiftee 출퇴근기록 + 휴가내역 xlsx 자동 다운로더
Playwright codegen 기반으로 작성 (2026-04-15)

실행:
  python shiftee_downloader.py            # 출퇴근 + 휴가 모두
  python shiftee_downloader.py attendance # 출퇴근만
  python shiftee_downloader.py leaves     # 휴가만
"""
import asyncio
import sys
from datetime import date
from pathlib import Path

from playwright.async_api import async_playwright

ROOT = Path(__file__).resolve().parent
INPUT_DIR = ROOT / "input"
INPUT_DIR.mkdir(exist_ok=True)

LOGIN_URL = "https://shiftee.io/ko/accounts/login"
ATTENDANCE_URL = "https://shiftee.io/app/companies/1943770/manager/attendances/list"
LEAVES_URL = "https://shiftee.io/app/companies/1943770/manager/leaves"


# ──────────────────────────────────────────────
# 유틸
# ──────────────────────────────────────────────

def get_payroll_period() -> tuple[date, date]:
    """실행일 기준 급여산정기간 (전월 25일 ~ 당월 24일)."""
    today = date.today()
    y, m, d = today.year, today.month, today.day
    if d <= 24:
        end = date(y, m, 24)
        start = date(y - 1, 12, 25) if m == 1 else date(y, m - 1, 25)
    else:
        start = date(y, m, 25)
        end = date(y + 1, 1, 24) if m == 12 else date(y, m + 1, 24)
    return start, end


def _load_credentials() -> tuple[str, str]:
    try:
        import shiftee_credentials as c
        return c.SHIFTEE_EMAIL, c.SHIFTEE_PASSWORD
    except ImportError:
        pass
    import os
    email = os.environ.get("SHIFTEE_EMAIL")
    password = os.environ.get("SHIFTEE_PASSWORD")
    if email and password:
        return email, password
    raise RuntimeError(
        "shiftee_credentials.py 파일 또는 SHIFTEE_EMAIL/SHIFTEE_PASSWORD 환경변수를 설정하세요."
    )


# ──────────────────────────────────────────────
# 로그인
# ──────────────────────────────────────────────

async def _login(page, email: str, password: str):
    print("  로그인 중...")
    await page.goto(LOGIN_URL)
    await page.get_by_role("textbox", name="이메일").fill(email)
    await page.get_by_role("textbox", name="비밀번호").fill(password)
    await page.get_by_role("button", name="로그인", exact=True).click()
    await page.wait_for_url("**/manager/**", timeout=15_000)
    print("  ✅ 로그인 완료")


# ──────────────────────────────────────────────
# 날짜 범위 설정
# ──────────────────────────────────────────────

async def _set_date_range(page, start: date, end: date):
    """
    bsdaterangepicker 날짜 범위 설정.
    - 피커를 열고, 현재 월 기준으로 ‹ 버튼을 클릭해 start 월로 이동
    - 왼쪽 패널에서 start.day 클릭, 오른쪽 패널에서 end.day 클릭
    """
    today = date.today()
    months_back = (today.year * 12 + today.month) - (start.year * 12 + start.month)

    # 다운로드 모달 내부의 bsdaterangepicker 클릭 (배경 페이지 date picker와 혼동 방지)
    await page.locator("sft-basic-export-modal input[bsdaterangepicker]").click()
    await page.wait_for_timeout(500)

    # start 월이 될 때까지 ‹ 클릭
    for _ in range(months_back):
        await page.get_by_role("button", name="‹").first.click()
        await page.wait_for_timeout(200)

    # 왼쪽 패널: start.day 클릭
    left = page.locator("bs-datepicker-container .bs-datepicker-body").nth(0)
    await left.locator("td span").filter(has_text=str(start.day)).first.click()
    await page.wait_for_timeout(300)

    # 오른쪽 패널: end.day 클릭 (start + 1개월 = end 월)
    right = page.locator("bs-datepicker-container .bs-datepicker-body").nth(1)
    await right.locator("td span").filter(has_text=str(end.day)).first.click()
    await page.wait_for_timeout(300)

    print(f"  → 기간: {start} ~ {end}")


# ──────────────────────────────────────────────
# 출퇴근기록 다운로드
# ──────────────────────────────────────────────

async def _download_attendance(page, start: date, end: date) -> Path:
    print("\n[출퇴근기록 다운로드]")
    await page.get_by_role("link", name="목록형").nth(1).click()
    await page.wait_for_timeout(1000)

    # 다운로드 드롭다운 → 출퇴근기록 EXCEL
    await page.get_by_role("button", name="다운로드").click()
    await page.get_by_text("출퇴근기록EXCEL").click()
    await page.wait_for_timeout(1000)

    # 직원 선택: 모두 선택 후 3명 제외
    await page.get_by_role("group").locator("a").filter(has_text="선택안됨").click()
    await page.wait_for_timeout(500)
    await page.get_by_role("group").get_by_text("모두 선택").click()
    await page.wait_for_timeout(400)
    print("  → 모두 선택")

    # 챈스(김찬범): 첫 번째 sft-checkbox
    await page.locator(
        ".dropdown-menu.sft-lg-dropdown-menu.show > .sft-dropdown-items > div > ul > "
        ".dropdown-item > label > sft-checkbox"
    ).first.click()
    await page.wait_for_timeout(300)

    # 데이브(변대현), 스티브(이석현): .sft-selected 순서대로
    await page.locator(".sft-container > .sft-selected").first.click()
    await page.wait_for_timeout(300)
    await page.locator(".sft-container > .sft-selected").first.click()
    await page.wait_for_timeout(300)
    print("  → 챈스/데이브/스티브 제외")

    # 드롭다운 닫기: JS로 .show 클래스 제거 (Angular Bootstrap dropdown)
    await page.evaluate(
        "document.querySelectorAll('.dropdown-menu.show').forEach(el => el.classList.remove('show'))"
    )
    await page.wait_for_timeout(400)

    # 날짜 설정
    await _set_date_range(page, start, end)

    # 다운로드
    filename = f"SHIFTEE-ATTENDANCES-{start.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}.xlsx"
    save_path = INPUT_DIR / filename
    async with page.expect_download(timeout=60_000) as dl_info:
        await page.get_by_role("dialog").get_by_role("button", name="다운로드").click()
    dl = await dl_info.value
    await dl.save_as(str(save_path))
    print(f"  ✅ 저장: {save_path.name}")
    return save_path


# ──────────────────────────────────────────────
# 휴가내역 다운로드
# ──────────────────────────────────────────────

async def _download_leaves(page, start: date, end: date) -> Path:
    print("\n[휴가내역 다운로드]")
    await page.get_by_role("link", name="휴가 내역").click()
    await page.wait_for_timeout(1000)

    # 다운로드 버튼
    await page.get_by_role("button", name="다운로드").click()
    await page.wait_for_timeout(500)

    # 직원 선택: 모두 선택 후 3명 제외
    await page.locator("a").filter(has_text="선택안됨").click()
    await page.wait_for_timeout(500)
    await page.get_by_role("group").get_by_text("모두 선택").click()
    await page.wait_for_timeout(400)
    print("  → 모두 선택")

    # 챈스(김찬범)
    await page.locator(".sft-container > .sft-selected").first.click()
    await page.wait_for_timeout(300)

    # 데이브(변대현)
    await page.get_by_role("group").get_by_text("데이브(변대현)차란 남양주센터").click()
    await page.wait_for_timeout(300)

    # 스티브(이석현)
    await page.get_by_role("listitem").filter(has_text="스티브(이석현)").locator("label").click()
    await page.wait_for_timeout(300)
    print("  → 챈스/데이브/스티브 제외")

    # 드롭다운 닫기: JS로 .show 클래스 제거
    await page.evaluate(
        "document.querySelectorAll('.dropdown-menu.show').forEach(el => el.classList.remove('show'))"
    )
    await page.wait_for_timeout(400)

    # 날짜 설정
    await _set_date_range(page, start, end)

    # 다운로드
    filename = f"SHIFTEE-LEAVES-{start.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}.xlsx"
    save_path = INPUT_DIR / filename
    async with page.expect_download(timeout=60_000) as dl_info:
        await page.get_by_role("dialog").get_by_role("button", name="다운로드").click()
    dl = await dl_info.value
    await dl.save_as(str(save_path))
    print(f"  ✅ 저장: {save_path.name}")
    return save_path


# ──────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────

async def run(mode: str = "all") -> dict[str, Path]:
    start, end = get_payroll_period()
    print(f"급여산정기간: {start} ~ {end}")

    email, password = _load_credentials()
    result = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=200)
        context = await browser.new_context(
            accept_downloads=True,
            viewport={"width": 1400, "height": 900},
        )
        page = await context.new_page()

        await _login(page, email, password)

        if mode in ("all", "attendance"):
            await page.goto(ATTENDANCE_URL, wait_until="networkidle", timeout=30_000)
            await page.wait_for_timeout(1000)
            result["attendance"] = await _download_attendance(page, start, end)

        if mode in ("all", "leaves"):
            await page.goto(LEAVES_URL, wait_until="networkidle", timeout=30_000)
            await page.wait_for_timeout(1000)
            result["leaves"] = await _download_leaves(page, start, end)

        await browser.close()

    return result


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    if mode not in ("all", "attendance", "leaves"):
        print("사용법: python shiftee_downloader.py [all|attendance|leaves]")
        sys.exit(1)
    try:
        result = asyncio.run(run(mode))
        print("\n=== 완료 ===")
        for k, v in result.items():
            print(f"  {k}: {v}")
    except KeyboardInterrupt:
        print("\n중단됨.")
        sys.exit(1)


if __name__ == "__main__":
    main()
