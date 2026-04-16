"""
이메일_발송용_정보 시트 기반 자동 급여명세서 발송기.

동작:
1) 터미널에서 YYYYMM(예: 202603)을 입력받아 해당 월의
   '{YYYY}년{MM:02d}월_이메일_발송용_정보' 시트를 찾습니다.
2) 터미널에서 로컬 PDF 폴더 경로를 입력받고,
   시트의 '이메일주소'(F)와 '첨부파일'(G) 값을 기준으로 PDF 파일명을 매칭해 첨부 발송합니다.

메일 발신 계정:
- 기본값은 'chance@mineis.io' 로, Gmail에서도 쓰는 계정으로 SMTP 발신이 가능합니다.
- Gmail SMTP 비밀번호는 보안상 '앱 비밀번호' 사용을 권장합니다.
"""

from __future__ import annotations

import argparse
import csv
import getpass
import hashlib
import os
import re
import smtplib
import sys
import time
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from pathlib import Path


def _prompt(msg: str, default: str | None = None) -> str:
    if default is None or default == "":
        return input(msg)
    v = input(f"{msg} [{default}] : ").strip()
    return v if v else default


def _parse_yyyymm(yyyy_mm: str) -> tuple[int, int]:
    s = str(yyyy_mm).strip()
    s = s.replace("-", "").replace("/", "").replace(" ", "")
    if len(s) != 6 or not s.isdigit():
        raise ValueError("YYYYMM은 6자리 숫자 예: 202603 형태로 입력하세요.")
    year = int(s[:4])
    month = int(s[4:])
    if not (1 <= month <= 12):
        raise ValueError("월은 01~12 범위여야 합니다.")
    return year, month


def _resolve_google_credentials_path(root: Path) -> str:
    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path and Path(env_path).exists():
        return env_path

    candidates = [
        root / "credentials" / "service_account.json",
        root / ".keys" / "ops-robot-keys.json",
    ]
    for p in candidates:
        if p.exists():
            return str(p)
    raise FileNotFoundError(
        "구글 시트 API 인증 파일을 찾지 못했습니다. "
        "예: 'credentials/service_account.json' 또는 '.keys/ops-robot-keys.json' "
        "또는 GOOGLE_APPLICATION_CREDENTIALS 환경변수를 설정하세요."
    )


def _open_google_sheet(spreadsheet_id: str):
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError as e:
        raise RuntimeError(
            "gspread, google-auth 패키지가 필요합니다. "
            "pip install gspread google-auth 로 설치하세요."
        ) from e

    root = Path(__file__).resolve().parent
    cred_path = _resolve_google_credentials_path(root)
    creds = Credentials.from_service_account_file(
        cred_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(spreadsheet_id)


def _sheet_title_from_yyyymm(year: int, month: int) -> str:
    return f"{year}년{month:02d}월_이메일_발송용_정보"


def _col_get(row: dict, key: str) -> str:
    v = row.get(key, "")
    if v is None:
        return ""
    s = str(v).strip()
    return s


def _safe_filename(name: str) -> str:
    # 실제 파일시스템에서 크게 문제 없도록 최소치 정리
    return str(name).strip().strip('"').strip("'")


def _mask_email(email: str) -> str:
    """
    개인정보 보호용 마스킹.
    예: chance@mineis.io -> ch***@mineis.io
    """
    e = (email or "").strip()
    if "@" not in e:
        return e
    local, domain = e.split("@", 1)
    local = local.strip()
    if not local:
        return f"***@{domain}"
    if len(local) <= 2:
        return f"{local[0]}***@{domain}"
    return f"{local[:2]}***@{domain}"


def _hash_value(s: str) -> str:
    return hashlib.sha256((s or "").strip().lower().encode("utf-8")).hexdigest()


_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def _redact_sensitive(text: str) -> str:
    # 에러 문자열 안에 이메일이 포함될 수 있으므로 마스킹
    return _EMAIL_RE.sub("[email-redacted]", text or "")


def _records_from_worksheet(ws) -> list[dict]:
    """
    gspread.get_all_records()는 헤더가 중복이면 예외를 던집니다.
    현재 이메일 시트는 '타입' 헤더가 2번 존재하므로(get_all_records 불가),
    get_all_values()로 직접 읽고 필요한 컬럼만 인덱스로 뽑습니다.
    """
    values = ws.get_all_values()  # [ [header...], [row...], ... ]
    if not values:
        return []

    header = [str(h).strip() for h in values[0]]

    needed_keys = [
        "이메일주소",
        "첨부파일",
        "텍스트_기본급",
        "텍스트_주휴수당",
        "텍스트_야근수당",
        "텍스트_무급휴가",
        "텍스트_추가근무수당",
    ]

    idx_by_key: dict[str, int] = {}
    for i, h in enumerate(header):
        if h in needed_keys and h not in idx_by_key:
            idx_by_key[h] = i

    records: list[dict] = []
    for row in values[1:]:
        rec: dict[str, str] = {}
        for k in needed_keys:
            idx = idx_by_key.get(k)
            if idx is None or idx >= len(row):
                rec[k] = ""
            else:
                rec[k] = str(row[idx]).strip()
        records.append(rec)
    return records


def _build_email_body(row: dict, year: int, month: int) -> tuple[str, str]:
    # 시트 컬럼은 '텍스트_기본급' 같은 문자열(예: '기본급 : 10시간 ...')로 들어있으므로
    # 템플릿의 '기본급 : ' 앞부분을 중복하지 않게 콜론 이후 값만 사용합니다.
    def _cell_after_colon(cell_value: str) -> str:
        t = (cell_value or "").strip()
        if not t or t.lower() == "nan":
            return ""
        if ":" in t:
            return t.split(":", 1)[1].strip()
        return t

    h_raw = _col_get(row, "텍스트_기본급")
    i_raw = _col_get(row, "텍스트_주휴수당")
    j_raw = _col_get(row, "텍스트_야근수당")
    k_raw = _col_get(row, "텍스트_무급휴가")

    h_val = _cell_after_colon(h_raw)
    i_val = _cell_after_colon(i_raw)
    j_val = _cell_after_colon(j_raw)
    k_val = _cell_after_colon(k_raw)

    bullet_lines: list[str] = []
    if h_val:
        bullet_lines.append(f"• 기본급 : {h_val}")
    if i_val:
        bullet_lines.append(f"• 주휴수당 : {i_val}")
    if j_val:
        bullet_lines.append(f"• 야근수당 : {j_val}")
    if k_val:
        bullet_lines.append(f"• 무급휴가 : {k_val}")

    y = year
    m = month  # 템플릿은 {MM} 그대로 사용(선행 0 제거)

    # 사용자가 준 템플릿을 그대로 두되, 빈 값이면 해당 '• ...' 줄만 제거합니다.
    # 사용자 요청: 제목은 고정
    subject = "[마인이스] 급여명세서 송부"
    if bullet_lines:
        calc_section = "\n".join(
            ["계산식은 아래와 같습니다:", *bullet_lines]
        )
        calc_section = f"{calc_section}\n"
    else:
        calc_section = ""

    body = "\n".join(
        [
            "안녕하세요,",
            "------",
            f"마인이스의 챈스입니다. {y}년 {m}월 급여명세서를 첨부해 보내드립니다.",
            "",
            calc_section.rstrip("\n"),
            "",
            "첨부파일이 없거나 깨지면 저에게 말씀해주시거나 010-9395-7133로 문자 부탁드립니다. 고생 많으셨습니다.",
            "",
            "감사합니다,",
            "챈스 드림",
        ]
    ).replace("\n\n\n", "\n\n").strip()

    return subject, body


def _send_one_email(
    *,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_pass: str,
    email_from: str,
    to_email: str,
    subject: str,
    body: str,
    pdf_path: Path,
    dry_run: bool,
):
    if dry_run:
        print(f"[DRY-RUN] to={_mask_email(to_email)} attach={pdf_path.name}")
        return

    msg = MIMEMultipart()
    msg["From"] = email_from
    msg["To"] = to_email
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)

    msg.attach(MIMEText(body, "plain", "utf-8"))

    # MIMEApplication: 바이너리 파일 첨부
    with open(pdf_path, "rb") as f:
        part = MIMEApplication(f.read(), _subtype="pdf")
    part.add_header("Content-Disposition", "attachment", filename=pdf_path.name)
    msg.attach(part)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=60) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(smtp_user, smtp_pass)
        server.sendmail(email_from, [to_email], msg.as_string())


def main():
    parser = argparse.ArgumentParser(description="이메일_발송용_정보 시트 기반 급여명세서 자동 발송기")
    parser.add_argument("--yyyyMM", default="", help="YYYYMM (예: 202603)")
    # pdf 경로는 '매번 바뀐다'는 전제이므로, 옵션으로 넘기지 않으면 터미널에서 묻습니다.
    parser.add_argument("--pdf-dir", default="", help="로컬 PDF 폴더 경로(미지정 시 터미널에서 입력받음)")
    parser.add_argument("--spreadsheet-id", default="", help="대상 구글 스프레드시트 ID")
    parser.add_argument("--dry-run", action="store_true", help="실제 발송하지 않고 매칭/로그만 수행")
    parser.add_argument("--sleep-sec", type=float, default=1.0, help="발송 간 대기시간")
    parser.add_argument("--skip-sent-log", action="store_true", help="로그에 성공 기록이 있으면 건너뜁니다.")
    parser.add_argument("--log-raw", action="store_true", help="로그에 개인정보를 원문으로 남깁니다. 기본은 마스킹입니다.")
    parser.add_argument(
        "--use-smtp-pass-env",
        action="store_true",
        help="SMTP 비밀번호를 환경변수(SMTP_PASS)에서 읽습니다. 기본은 항상 터미널 프롬프트 입력입니다.",
    )
    parser.add_argument(
        "--show-smtp-pass",
        action="store_true",
        help="보안상 비권장: SMTP 비밀번호 입력을 가리지 않고(입력 내용이 터미널에 표시) 입력받습니다.",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parent

    # spreadsheet_id 기본값은 기존 생성 코드의 TARGET_SPREADSHEET_ID를 사용합니다.
    spreadsheet_id = args.spreadsheet_id.strip()
    if not spreadsheet_id:
        try:
            import google_sheet_exporter

            spreadsheet_id = getattr(google_sheet_exporter, "TARGET_SPREADSHEET_ID", "")
        except Exception:
            spreadsheet_id = ""
    if not spreadsheet_id:
        spreadsheet_id = _prompt("대상 스프레드시트 ID를 입력하세요 (예: 18skw...)", default="")

    yyyy_mm = args.yyyyMM.strip()
    if not yyyy_mm:
        yyyy_mm = _prompt("YYYYMM을 입력하세요 (예: 202603)")

    year, month = _parse_yyyymm(yyyy_mm)
    ws_title = _sheet_title_from_yyyymm(year, month)
    print(f"사용 시트 제목: {ws_title}")

    pdf_dir = args.pdf_dir.strip()
    if not pdf_dir:
        pdf_dir = _prompt("로컬 PDF 폴더 경로를 입력하세요")
    pdf_dir_path = Path(pdf_dir).expanduser().resolve()
    if not pdf_dir_path.exists() or not pdf_dir_path.is_dir():
        raise NotADirectoryError(f"PDF 폴더가 아닙니다: {pdf_dir_path}")

    # 메일 발신 설정
    email_from = _prompt("발신 계정 이메일 (기본: chance@mineis.io)", default="chance@mineis.io").strip()
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com").strip()
    smtp_port = int(os.environ.get("SMTP_PORT", "587").strip())

    smtp_user = os.environ.get("SMTP_USER", email_from).strip()
    smtp_pass = ""
    if args.use_smtp_pass_env:
        smtp_pass = os.environ.get("SMTP_PASS", "").strip()
    # 기본은 "지금처럼" 프롬프트 입력으로만 받기(비밀번호 노출 위험 최소화)
    if not smtp_pass:
        if args.show_smtp_pass:
            # 사용자가 보이길 원해서만 예외적으로 echo되는 입력을 허용
            smtp_pass = input("SMTP 비밀번호(권장: 앱 비밀번호)를 입력하세요: ").strip()
        else:
            smtp_pass = getpass.getpass("SMTP 비밀번호(권장: 앱 비밀번호)를 입력하세요: ")

    # 실제 발송 여부 확인
    dry_run = args.dry_run
    if not dry_run:
        yn = _prompt("정말로 실제 발송할까요? (y/N)", default="N").lower()
        dry_run = yn not in ("y", "yes")

    # 첨부파일 매칭을 위해 폴더 내 파일 인덱스 구성(대/소문자 무시)
    file_index = {p.name.lower(): p for p in pdf_dir_path.iterdir() if p.is_file()}

    # 구글 시트 로드 (월 제목이 다를 경우를 대비해 대화형 폴백 처리)
    sh = _open_google_sheet(spreadsheet_id)
    try:
        ws = sh.worksheet(ws_title)
    except Exception:
        # WorksheetNotFound 포함 (gspread 예외는 버전에 따라 클래스가 다를 수 있어 broad하게 처리)
        titles = [w.title for w in sh.worksheets()]
        print("해당 시트를 찾지 못했습니다. 사용 가능한 시트 제목(일부):")
        for t in titles[:50]:
            print(f"- {t}")
        ws_title2 = _prompt("사용할 시트 제목을 정확히 입력하세요")
        ws = sh.worksheet(ws_title2)

    records = _records_from_worksheet(ws)
    print(f"시트 레코드 수: {len(records)}")

    # 로그 파일
    log_path = root / f"email_send_log_{year}{month:02d}.csv"
    # 성공 기록 세트 로딩
    already_sent: set[tuple[str, str]] = set()
    if args.skip_sent_log and log_path.exists():
        with open(log_path, "r", encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                if row.get("status", "").lower() == "success":
                    # 마스킹/해시 버전 대응
                    to_hash = (row.get("to_email_hash") or "").strip()
                    if not to_hash:
                        to_hash = _hash_value(row.get("to_email", ""))
                    attach_norm = row.get("attach_name", "").strip().lower()
                    if to_hash and attach_norm:
                        already_sent.add((to_hash, attach_norm))

    sent_count = 0
    skip_count = 0
    missing_pdf_count = 0
    fail_count = 0

    # 로그 헤더 (처음 생성될 때만)
    log_exists = log_path.exists()
    with open(log_path, "a", encoding="utf-8-sig", newline="") as f_log:
        fieldnames = ["timestamp", "to_email", "to_email_hash", "attach_name", "status", "error"]
        w = csv.DictWriter(f_log, fieldnames=fieldnames)
        if not log_exists:
            w.writeheader()

        for row in records:
            to_email = _col_get(row, "이메일주소")
            attach_name = _col_get(row, "첨부파일")

            if not to_email or not attach_name:
                skip_count += 1
                continue

            attach_name = _safe_filename(attach_name)
            to_hash = _hash_value(to_email)
            key = (to_hash, attach_name.lower())
            if key in already_sent:
                skip_count += 1
                continue

            pdf_path = file_index.get(attach_name.lower())
            if not pdf_path:
                missing_pdf_count += 1
                to_email_log = to_email if args.log_raw else _mask_email(to_email)
                w.writerow(
                    {
                        "timestamp": datetime.now().isoformat(),
                        "to_email": to_email_log,
                        "to_email_hash": to_hash,
                        "attach_name": attach_name,
                        "status": "missing_pdf",
                        "error": "",
                    }
                )
                continue

            subject, body = _build_email_body(row, year, month)

            try:
                _send_one_email(
                    smtp_host=smtp_host,
                    smtp_port=smtp_port,
                    smtp_user=smtp_user,
                    smtp_pass=smtp_pass,
                    email_from=email_from,
                    to_email=to_email,
                    subject=subject,
                    body=body,
                    pdf_path=pdf_path,
                    dry_run=dry_run,
                )
                if dry_run:
                    status = "dry_run_success"
                else:
                    status = "success"
                    sent_count += 1
                to_email_log = to_email if args.log_raw else _mask_email(to_email)
                w.writerow(
                    {
                        "timestamp": datetime.now().isoformat(),
                        "to_email": to_email_log,
                        "to_email_hash": to_hash,
                        "attach_name": attach_name,
                        "status": status,
                        "error": "",
                    }
                )
            except Exception as e:
                fail_count += 1
                to_email_log = to_email if args.log_raw else _mask_email(to_email)
                err_redacted = _redact_sensitive(str(e))
                w.writerow(
                    {
                        "timestamp": datetime.now().isoformat(),
                        "to_email": to_email_log,
                        "to_email_hash": to_hash,
                        "attach_name": attach_name,
                        "status": "fail",
                        "error": err_redacted,
                    }
                )
                print(f"[FAIL] attach={attach_name} err={err_redacted}")
            finally:
                time.sleep(max(args.sleep_sec, 0.0))

    print(
        "완료: "
        f"sent={sent_count}, skip={skip_count}, missing_pdf={missing_pdf_count}, fail={fail_count}, log={log_path}"
    )


if __name__ == "__main__":
    main()

