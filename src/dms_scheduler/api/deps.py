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
    operator_token: str | None = Header(default=None, alias="X-Operator-Token"),
    settings=Depends(get_settings),
) -> None:
    if (
        not operator_token
        or not settings.operator_token
        or operator_token != settings.operator_token
    ):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid operator token")
