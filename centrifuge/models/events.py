from typing import Optional

from pydantic import BaseModel, Field


class CentrifugeEvent(BaseModel):
    pass


class ConnectingEvent(CentrifugeEvent):
    """
    ConnectingEvent is a connecting event context passed to on_connecting callback.
    """

    code: int
    reason: str


class ConnectedEvent(CentrifugeEvent):
    """
    ConnectedEvent is a connected event context passed to OnConnected callback.
    """

    client_id: str = ""
    version: str = ""
    data: Optional[bytes] = Field(None)


class DisconnectEvent(CentrifugeEvent):
    """
    DisconnectedEvent is a disconnected event context passed to OnDisconnected callback.
    """

    code: int
    reason: str


class ErrorEvent(CentrifugeEvent):
    """
    ErrorEvent is an error event context passed to on_error callback.
    """

    error: str


class RefreshEvent(ErrorEvent):
    pass
