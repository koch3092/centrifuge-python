import asyncio
from asyncio import TimerHandle
from typing import Awaitable, Callable, Dict, Optional

from centrifuge.client import Client
from centrifuge.models.errors import ConfigurationError
from centrifuge.models.events import SubscriptionTokenEvent

SUB_STATUS_UNSUBSCRIBED = "unsubscribed"
SUB_STATUS_SUBSCRIBING = "subscribing"
SUB_STATUS_SUBSCRIBED = "subscribed"


class SubscriptionConfig:
    """
    SubscriptionConfig allows setting Subscription options.
    """

    def __init__(self, **kwargs):
        """
        :param data: data is an arbitrary data to pass to a server in each subscribe request.
        :param token: token for Subscription.
        :param get_token: get_token called to get or refresh private channel subscription token.
        :param positioned: positioned flag asks server to make Subscription positioned. Only makes sense
        in channels with history stream on.
        :param recoverable: recoverable flag asks server to make Subscription recoverable. Only makes sense
        in channels with history stream on.
        :param join_leave: join_leave flag asks server to push join/leave messages.
        """
        self.data: bytes = kwargs.get("data", None)
        self.token: str = kwargs.get("token", "")
        self.get_token: Callable[[SubscriptionTokenEvent], Awaitable] = kwargs.get(
            "get_token", None
        )
        self.positioned: bool = kwargs.get("positioned", False)
        self.recoverable: bool = kwargs.get("recoverable", False)
        self.join_leave: bool = kwargs.get("join_leave", False)


class Subscription:
    min_delay = 0.2
    max_delay = 60
    factor = 2
    jitter = True

    def __init__(
        self,
        centrifuge: Client,
        channel: str,
        config: SubscriptionConfig = None,
        **kwargs,
    ):
        """
        :param centrifuge: centrifuge client instance.
        :param channel: channel name to subscribe.
        :param config: SubscriptionConfig.
        """
        self._centrifuge: Client = centrifuge
        self._channel: str = channel
        self._status: str = SUB_STATUS_UNSUBSCRIBED

        if config:
            self._data: bytes = config.data
            self._positioned: bool = config.positioned
            self._recoverable: bool = config.recoverable
            self._join_leave: bool = config.join_leave
            self._token: str = config.token
            self._get_token: Callable[
                [SubscriptionTokenEvent], Awaitable
            ] = config.get_token

        self._offset: int = 0
        self._epoch: str = ""
        self._recover: bool = False

        self._resubscribe_timer: Optional[TimerHandle] = None
        self._refresh_timer: Optional[TimerHandle] = None

        self._events: Dict[str, Callable] = {
            "on_subscribing": kwargs.get("on_subscribing"),
            "on_subscribed": kwargs.get("on_subscribed"),
            "on_unsubscribe": kwargs.get("on_unsubscribe"),
            "on_error": kwargs.get("on_error"),
            "on_publication": kwargs.get("on_publication"),
            "on_join": kwargs.get("on_join"),
            "on_leave": kwargs.get("on_leave"),
        }

        self._check_handlers()

    def _check_handlers(self):
        for key, handler in self._events.items():
            if handler and not asyncio.iscoroutinefunction(handler):
                raise ConfigurationError(f"handler {key} must be coroutine function")

    def state(self):
        return self._status
