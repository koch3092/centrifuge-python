from typing import Optional

from pydantic import BaseModel

from centrifuge.models.client import StreamPosition


class HistoryOptions(BaseModel):
    limit: Optional[int] = 0
    since: Optional[StreamPosition] = None
    reverse: Optional[bool] = False
