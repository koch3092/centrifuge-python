from typing import Dict, Optional

from pydantic import BaseModel


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


class Command(BaseModel):
    id: Optional[int] = None

    connect: Optional[ConnectRequest] = None
    subscribe: Optional[SubscribeRequest] = None
    refresh: Optional[RefreshRequest] = None
