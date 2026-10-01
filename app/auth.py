"""Token-derived identity.

Every authenticated endpoint takes the caller's identity *only* from the verified
JWT `sub` claim. Request bodies never carry identity; if a client sends a `user_id`
field it is ignored by the schema and has no effect.

`POST /auth/token` is a stand-in for a real identity provider so reviewers can mint
tokens for many users when load testing. In production this endpoint would not exist
and tokens would come from the IdP (same verification path).
"""
import hmac
import re
import time
from dataclasses import dataclass

import jwt
from fastapi import APIRouter, Header
from pydantic import BaseModel, Field

from .config import settings
from .errors import ApiError

router = APIRouter()
_ALG = "HS256"
USER_ID_RE = re.compile(r"^[A-Za-z0-9_.@:-]{1,64}$")


@dataclass(frozen=True)
class Principal:
    user_id: str
    is_admin: bool


def issue_token(user_id: str, admin: bool = False) -> str:
    now = int(time.time())
    claims = {"sub": user_id, "adm": admin, "iat": now, "exp": now + settings.jwt_ttl_s}
    return jwt.encode(claims, settings.jwt_secret, algorithm=_ALG)


def _principal_from_header(authorization: str | None) -> Principal:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise ApiError(401, "unauthenticated", "missing bearer token")
    token = authorization[7:].strip()
    try:
        claims = jwt.decode(token, settings.jwt_secret, algorithms=[_ALG], options={"require": ["sub", "exp"]})
    except jwt.PyJWTError:
        raise ApiError(401, "unauthenticated", "invalid or expired token") from None
    sub = claims.get("sub")
    if not isinstance(sub, str) or not USER_ID_RE.match(sub):
        raise ApiError(401, "unauthenticated", "invalid subject")
    return Principal(user_id=sub, is_admin=bool(claims.get("adm")))


async def current_user(authorization: str | None = Header(default=None)) -> Principal:
    return _principal_from_header(authorization)


async def current_admin(authorization: str | None = Header(default=None)) -> Principal:
    p = _principal_from_header(authorization)
    if not p.is_admin:
        raise ApiError(403, "forbidden", "admin token required")
    return p


class TokenRequest(BaseModel):
    user_id: str = Field(pattern=USER_ID_RE.pattern)
    admin: bool = False
    admin_key: str | None = None


@router.post("/auth/token")
async def mint_token(req: TokenRequest):
    if not settings.demo_auth:
        raise ApiError(404, "not_found", "demo token issuer disabled")
    if req.admin and settings.admin_key:
        if not req.admin_key or not hmac.compare_digest(req.admin_key, settings.admin_key):
            raise ApiError(403, "forbidden", "bad admin_key")
    return {"token": issue_token(req.user_id, req.admin), "user_id": req.user_id,
            "admin": req.admin, "expires_in": settings.jwt_ttl_s}
