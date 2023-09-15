from typing import Dict, Optional

from pydantic import BaseModel

from centrifuge.models.client import StreamPosition


class SubscribeRequest(BaseModel):
    channel: str
    token: Optional[str]
    recover: Optional[bool]
    epoch: Optional[str]
    offset: Optional[int]
    data: Optional[dict]
    positioned: Optional[bool]
    recoverable: Optional[bool]
    join_level: Optional[bool]


class ConnectRequest(BaseModel):
    token: Optional[str] = ""
    name: Optional[str] = "python"
    version: Optional[str] = ""
    data: Optional[dict] = None
    subs: Optional[Dict[str, SubscribeRequest]] = None


class RefreshRequest(BaseModel):
    token: Optional[str] = ""


class PublishRequest(BaseModel):
    channel: str = ""
    data: Optional[bytes] = None


class HistoryRequest(BaseModel):
    channel: str = ""
    limit: int = 0
    since: Optional[StreamPosition] = None
    reverse: bool = False


class Command(BaseModel):
    id: Optional[int] = None

    connect: Optional[ConnectRequest] = None
    subscribe: Optional[SubscribeRequest] = None
    refresh: Optional[RefreshRequest] = None
    publish: Optional[PublishRequest] = None
    history: Optional[HistoryRequest] = None
