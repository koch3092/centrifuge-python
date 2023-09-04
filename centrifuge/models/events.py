from typing import Optional

from pydantic import BaseModel, Field

from centrifuge.models.client import StreamPosition
from centrifuge.models.reply import Publication


class CentrifugeEvent(BaseModel):
    pass


class ConnectionTokenEvent(CentrifugeEvent):
    """
    ConnectionTokenEvent may contain some useful contextual information in the future.
    For now, it's empty.
    """

    pass


class SubscriptionTokenEvent(CentrifugeEvent):
    """
    SubscriptionTokenEvent contains info required to get subscription token when client wants to subscribe on private
    channel.
    """

    channel: str


class ServerPublicationEvent(CentrifugeEvent, Publication):
    channel: str


class ServerSubscribedEvent(CentrifugeEvent):
    channel: str
    was_recovering: bool
    recovered: bool
    recoverable: bool
    positioned: bool
    stream_position: Optional[StreamPosition] = Field(None)
    data: Optional[bytes] = Field(None)


class ServerUnsubscribedEvent(CentrifugeEvent):
    channel: str


class ServerSubscribingEvent(CentrifugeEvent):
    channel: str


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
