from pydantic import BaseModel


class DisconnectState(BaseModel):
    code: int
    reason: str
    reconnect: bool
