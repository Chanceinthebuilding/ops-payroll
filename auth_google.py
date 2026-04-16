"""
Google OAuth 로그인 (Flask-Dance).

환경 변수:
  GOOGLE_OAUTH_CLIENT_ID / GOOGLE_OAUTH_CLIENT_SECRET — Google Cloud Console OAuth 클라이언트
  SECRET_KEY — Flask 세션 서명 (배포 시 반드시 임의 문자열)
  AUTH_DISABLED=1 — OAuth 없이 기존처럼 열기 (로컬 전용)
  ALLOWED_EMAIL_DOMAIN — 예: company.com (비우면 모든 Google 계정 허용)
  GOOGLE_OAUTH_HOSTED_DOMAIN — Workspace 한정 로그인 시 도메인 (선택, hd 파라미터)

리디렉션 URI (Google 콘솔에 등록): https://<호스트>/login/google/authorized
"""
from __future__ import annotations

import os

from flask import flash, redirect, request, session, url_for
from flask_dance.contrib.google import google, make_google_blueprint
from flask_dance.consumer.storage.session import SessionStorage


def auth_disabled() -> bool:
    return os.environ.get("AUTH_DISABLED", "").strip().lower() in ("1", "true", "yes")


def allowed_email_domain() -> str:
    return os.environ.get("ALLOWED_EMAIL_DOMAIN", "").strip().lower()


def _domain_ok(email: str) -> bool:
    dom = allowed_email_domain()
    if not dom:
        return bool(email)
    if not email:
        return False
    return email.lower().endswith("@" + dom)


def register_google_auth(app) -> None:
    if auth_disabled():
        app.config["_AUTH_DISABLED"] = True

        @app.context_processor
        def inject_auth_disabled():
            return {
                "auth_enabled": False,
                "current_user_email": None,
                "current_user_name": None,
                "is_admin_user": True,
            }

        return

    client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "").strip()
    client_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise RuntimeError(
            "Google OAuth: GOOGLE_OAUTH_CLIENT_ID / GOOGLE_OAUTH_CLIENT_SECRET 을 설정하거나 "
            "AUTH_DISABLED=1 로 로컬 개발을 진행하세요."
        )

    hd = os.environ.get("GOOGLE_OAUTH_HOSTED_DOMAIN", "").strip() or None

    google_bp = make_google_blueprint(
        client_id=client_id,
        client_secret=client_secret,
        scope=[
            "openid",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/userinfo.profile",
        ],
        storage=SessionStorage(),
        redirect_to="index",
        hosted_domain=hd,
    )
    app.register_blueprint(google_bp, url_prefix="/login")

    @app.before_request
    def _auth_gate():
        ep = request.endpoint or ""
        if ep == "static" or (ep and ep.startswith("google")):
            return None
        if ep in ("login_page", "logout", "healthz"):
            return None

        if session.get("user_email"):
            return None

        if not google.authorized:
            return redirect(url_for("login_page"))

        resp = google.get("/oauth2/v2/userinfo")
        if not resp.ok:
            flash("Google 계정 정보를 가져오지 못했습니다. 다시 로그인해 주세요.", "error")
            return redirect(url_for("login_page"))

        data = resp.json()
        email = (data.get("email") or "").strip()
        if not _domain_ok(email):
            session.clear()
            dom = allowed_email_domain()
            msg = (
                f"허용되지 않은 계정입니다. (@{dom} 만 가능)"
                if dom
                else "이메일을 확인할 수 없습니다."
            )
            flash(msg, "error")
            return redirect(url_for("login_page"))

        session["user_email"] = email
        session["user_name"] = (data.get("name") or "").strip()
        session.permanent = True
        return None

    @app.context_processor
    def inject_auth():
        return {
            "auth_enabled": True,
            "current_user_email": session.get("user_email"),
            "current_user_name": session.get("user_name"),
        }
