from fastapi import Depends, Header, HTTPException, status

from ..core.settings import get_settings
from ..services.state_store import StateStore

_state_store: StateStore | None = None


def get_state_store() -> StateStore:
    global _state_store
    if _state_store is None:
        _state_store = StateStore()
    return _state_store


def require_operator_token(
    authorization: str | None = Header(default=None, alias="Authorization"),
    settings=Depends(get_settings),
) -> None:
    token = None
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1]
    if not token or not settings.operator_token or token != settings.operator_token:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid operator token")
