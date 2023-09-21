from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from centrifuge.models.push import Push


class Error(BaseModel):
    code: int = None
    message: str = None
    temporary: Optional[bool] = Field(False)


class ClientInfo(BaseModel):
    user: str
    client: str
    conn_info: str = Field(default=None, alias="connInfo")
    chan_info: str = Field(default=None, alias="chanInfo")


class CentrifugeResult(BaseModel):
    pass


class Publication(CentrifugeResult):
    data: str = Field(default=None)
    info: ClientInfo = Field(default=None)
    offset: int = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)


class SubscribeResult(CentrifugeResult):
    expires: bool = Field(default=False)
    ttl: int = Field(default=0)
    recoverable: bool = Field(default=False)
    epoch: str = Field(default="")
    publications: List[Publication] = Field(default={})
    recovered: bool = Field(default=False)
    offset: int = Field(default=0)
    positioned: bool = Field(default=False)
    data: bytes = Field(default=None)
    was_recovering: bool = Field(default=False, alias="wasRecovering")


class RefreshResult(CentrifugeResult):
    client: str = Field(default=None)
    version: str = Field(default=None)
    expires: bool = Field(default=None)
    ttl: int = Field(default=None)


class ConnectResult(CentrifugeResult):
    client: str = None
    version: str = None
    expires: bool = None
    ttl: int = None
    data: bytes = None
    subs: Optional[Dict[str, SubscribeResult]] = Field({})
    ping: int = None
    pong: bool = None
    session: Optional[str] = Field(None)
    node: Optional[str] = Field(None)


class PublishResult(CentrifugeResult):
    pass


class HistoryResult(CentrifugeResult):
    publications: List[Publication] = Field(default=[])
    offset: int = Field(default=0)
    epoch: str = Field(default="")


class PresenceResult(CentrifugeResult):
    clients: Dict[str, ClientInfo] = Field(default={})


class PresenceStats(BaseModel):
    num_clients: int = 0
    num_users: int = 0


class PresenceStatsResult(BaseModel):
    presence_stats: PresenceStats = Field(default=None, alias="presenceStats")


class Reply(BaseModel, extra="allow"):
    id: Optional[int] = Field(None)
    error: Optional[Error] = Field(None)

    push: Optional[Push] = Field(None)
    connect: Optional[ConnectResult] = Field(None)
    refresh: Optional[RefreshResult] = Field(None)

    publish: Optional[PublishResult] = Field(None)
    history: Optional[HistoryResult] = Field(None)
    presence: Optional[PresenceResult] = Field(None)
    presence_stats: Optional[PresenceStatsResult] = Field(None)
