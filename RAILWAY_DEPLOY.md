# Railway 배포 절차

## 1) 사전 준비

- Git 저장소를 GitHub에 푸시
- Google Cloud Console에서 OAuth 클라이언트(웹 애플리케이션) 생성
- Railway 계정 생성

## 2) Railway 프로젝트 생성

1. Railway Dashboard에서 `New Project` 클릭
2. `Deploy from GitHub repo` 선택
3. 이 저장소 선택 후 배포 시작
4. 첫 배포 로그에서 `gunicorn app:app` 시작 확인

이 저장소에는 Railway용 `Procfile`/`railway.json`이 포함되어 있어 기본 실행 커맨드가 자동 적용됩니다.

## 3) 환경 변수 설정

Railway 서비스의 `Variables`에서 아래 값 입력:

- `SECRET_KEY` = 긴 랜덤 문자열
- `SESSION_COOKIE_SECURE` = `1`
- `GOOGLE_OAUTH_CLIENT_ID` = Google OAuth Client ID
- `GOOGLE_OAUTH_CLIENT_SECRET` = Google OAuth Client Secret
- `ADMIN_EMAILS` = (필수) 콤마로 구분한 관리자 Gmail/Workspace 이메일 (예: `you@company.com`) — `/admin` 업로드·구글 시트 내보내기
- `ALLOWED_EMAIL_DOMAIN` = (선택) `yourcompany.com`
- `GOOGLE_OAUTH_HOSTED_DOMAIN` = (선택) `yourcompany.com`

로컬 개발에서만 필요:

- `AUTH_DISABLED=1` (Railway 배포에서는 설정하지 않음)

## 4) 도메인 연결

1. Railway `Settings > Domains`에서 도메인 추가
2. 안내된 DNS 레코드(CNAME/A) 등록
3. SSL 발급 완료 상태 확인

## 5) Google OAuth 리디렉션 URI 등록

Google Cloud Console의 OAuth 클라이언트에 아래 URI 추가:

- `https://<배포도메인>/login/google/authorized`
- (로컬 유지 시) `http://127.0.0.1:5000/login/google/authorized`

## 6) 배포 검증 체크리스트

- `https://<배포도메인>/healthz` 가 `{"ok": true}` 응답
- 미로그인 상태에서 `/` 접속 시 `/login` 이동
- Google 로그인 후 업로드 화면 접근 가능
- 엑셀 업로드 후 결과 화면 진입 가능

## 7) 운영 팁

- 기본 `output/`은 로컬 디스크 기반이라 재배포/재시작 시 유실될 수 있음
- 결과 보존이 필요하면 추후 Object Storage/S3 또는 DB 저장으로 확장 권장
- 배포 후에는 `SECRET_KEY`, OAuth 시크릿 회전 정책을 주기적으로 운영 권장
