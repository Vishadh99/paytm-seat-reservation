"""Domain errors. Every expected outcome (decline, bad input, auth) is a 4xx with a
stable machine-readable `error` code — 5xx is reserved for genuine bugs/outages."""
import logging

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse


class ApiError(Exception):
    def __init__(self, status: int, error: str, message: str, **extra):
        self.status = status
        self.error = error
        self.message = message
        self.extra = extra

    def body(self) -> dict:
        return {"error": self.error, "message": self.message, **self.extra}


async def api_error_handler(_: Request, exc: ApiError) -> JSONResponse:
    return JSONResponse(status_code=exc.status, content=exc.body())


async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={"error": "invalid_request", "message": "request failed validation",
                 "details": [{"loc": list(e.get("loc", [])), "msg": e.get("msg")} for e in exc.errors()]},
    )


async def dependency_down_handler(_: Request, exc: Exception) -> JSONResponse:
    """DB unreachable / pool exhausted past its timeout: fail closed with 503 + Retry-After.
    We never guess an answer without the system of record (CP, not AP)."""
    logging.getLogger("seats").error("dependency unavailable", extra={"fields": {"error": repr(exc)}})
    return JSONResponse(status_code=503, headers={"Retry-After": "1"},
                        content={"error": "unavailable", "message": "reservation store unavailable, retry"})
