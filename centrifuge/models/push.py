from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class ClientInfo(BaseModel):
    user: str
    client: str
    conn_info: str = Field(default=None, alias="connInfo")
    chan_info: str = Field(default=None, alias="chanInfo")


class Publication(BaseModel):
    data: str = Field(default=None)
    info: ClientInfo = Field(default=None)
    offset: int = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)


class Subscribe(BaseModel):
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


class Refresh(BaseModel):
    client: str = Field(default=None)
    version: str = Field(default=None)
    expires: bool = Field(default=None)
    ttl: int = Field(default=None)


class Connect(BaseModel):
    client: str = None
    version: str = None
    expires: bool = None
    ttl: int = None
    data: bytes = None
    subs: Optional[Dict[str, Subscribe]]
    ping: int = None
    pong: bool = None
    session: Optional[str]
    node: Optional[str]


class Push(BaseModel):
    channel: str

    pub: Optional[Publication] = None
    connect: Optional[Connect] = None
    refresh: Optional[Refresh] = None
