from typing import Callable, Optional, Awaitable

from pydantic import BaseModel, Field

from centrifuge.models.errors import TransportError
from centrifuge.models.reply import Reply


class ServerSub(BaseModel):
    offset: Optional[int] = Field(None)
    epoch: Optional[str] = Field(None)
    recoverable: Optional[bool] = Field(None)


class Request(BaseModel):
    cb: Callable[[Optional[Reply], Optional[TransportError]], Awaitable]


class StreamPosition(BaseModel):
    offset: int
    epoch: str


class ConnectFuture(BaseModel):
    fn: Callable[[Optional[TransportError]], Awaitable]
