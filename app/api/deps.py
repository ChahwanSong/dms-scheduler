from fastapi import Depends
from ..services.state_store import StateStore

_state_store: StateStore | None = None


def get_state_store() -> StateStore:
    global _state_store
    if _state_store is None:
        _state_store = StateStore()
    return _state_store
