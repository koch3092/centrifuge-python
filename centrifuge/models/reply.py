from typing import Awaitable, Callable, Dict, List, Optional

from pydantic import BaseModel, Field

from centrifuge.models.errors import TransportError
from centrifuge.models.push import Push


class Error(BaseModel):
    code: int = None
    message: str = None
    temporary: Optional[bool]


class ClientInfo(BaseModel):
    user: str
    client: str
    conn_info: str = Field(default=None, alias="connInfo")
    chan_info: str = Field(default=None, alias="chanInfo")


class Publication(BaseModel):
    data: str = Field(default=None)
    info: ClientInfo = Field(default=None)
    offset: int = Field(default=None)
    tags: Dict[str, str] = Field(default=None)


class SubscribeResult(BaseModel):
    expires: bool = Field(default=None)
    ttl: int = Field(default=None)
    recoverable: bool = Field(default=None)
    epoch: str = Field(default=None)
    publications: List[Publication] = Field(default=None)
    recovered: bool = Field(default=None)
    offset: int = Field(default=None)
    positioned: bool = Field(default=None)
    data: bytes = Field(default=None)
    was_recovering: bool = Field(default=None, alias="wasRecovering")


class RefreshResult(BaseModel):
    client: str = Field(default=None)
    version: str = Field(default=None)
    expires: bool = Field(default=None)
    ttl: int = Field(default=None)


class ConnectResult(BaseModel):
    client: str = None
    version: str = None
    expires: bool = None
    ttl: int = None
    data: bytes = None
    subs: Optional[Dict[str, SubscribeResult]] = Field(None)
    ping: int = None
    pong: bool = None
    session: Optional[str] = Field(None)
    node: Optional[str] = Field(None)


class Reply(BaseModel, extra="allow"):
    id: Optional[int] = Field(None)
    error: Optional[Error] = Field(None)

    push: Optional[Push] = Field(None)
    connect: Optional[ConnectResult] = Field(None)
    refresh: Optional[RefreshResult] = Field(None)


class Request(BaseModel):
    cb: Callable[[Optional[Reply], Optional[TransportError]], Awaitable]
