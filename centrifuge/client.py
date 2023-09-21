import asyncio
import random
from asyncio import CancelledError, Event, TimerHandle
from typing import Callable, Dict, List, Optional, Set, Awaitable

import websockets
from pydantic import AnyUrl
from websockets.exceptions import ConnectionClosedError, WebSocketException

from centrifuge.codes import (
    CONNECTING_CONNECT_CALLED,
    CONNECTING_NO_PING,
    CONNECTING_TRANSPORT_CLOSED,
    DISCONNECTED_DISCONNECT_CALLED,
    DISCONNECTED_UNAUTHORIZED,
)
from centrifuge.config import Config
from centrifuge.models.client import Request, ServerSub, StreamPosition, ConnectFuture
from centrifuge.models.command import (
    Command,
    ConnectRequest,
    RefreshRequest,
    PublishRequest,
    HistoryRequest,
    PresenceRequest,
    PresenceStatsRequest,
)
from centrifuge.models.errors import (
    ClientClosed,
    ClientDisconnected,
    ConfigurationError,
    Timeout,
    TransportError,
    CentrifugeServerError,
)
from centrifuge.models.events import (
    CentrifugeEvent,
    ConnectedEvent,
    ConnectingEvent,
    DisconnectEvent,
    ErrorEvent,
    RefreshEvent,
    ConnectionTokenEvent,
    ServerSubscribedEvent,
    ServerPublicationEvent,
    ServerUnsubscribedEvent,
    ServerSubscribingEvent,
)
from centrifuge.models.push import Push
from centrifuge.models.reply import (
    Reply,
    PublishResult,
    HistoryResult,
    PresenceResult,
    PresenceStatsResult,
)
from centrifuge.models.subscription import HistoryOptions
from centrifuge.models.transport import DisconnectState
from centrifuge.queue import CBQueue
from centrifuge.transport import WebsocketTransport

STATUS_CONNECTED = "connected"
STATUS_CONNECTING = "connecting"
STATUS_DISCONNECTED = "disconnected"
STATUS_CLOSED = "closed"


class Client:
    factor = 2
    base_delay = 1
    max_delay = 60
    jitter = 0.5

    def __init__(self, endpoint: AnyUrl, config: Config, **kwargs):
        self._future_id = 0
        self._cmd_id = 0
        self.endpoint: AnyUrl = endpoint
        self.config: Config = config
        self._token: str = self.config.token
        self._data: List[bytes] = self.config.data
        self._status: str = kwargs.get("status", STATUS_DISCONNECTED)
        self._server_subs: Dict[str, ServerSub] = {}
        self._delay_ping: Event = kwargs.get("delay_ping", Event())
        self._send_pong: bool = kwargs.get("send_pong", False)
        self._requests: Dict[int, Request] = kwargs.get("requests", {})
        self._cb_queue: CBQueue = kwargs.get("cb_queue", CBQueue())
        self._transport: Optional[WebsocketTransport] = None
        self._refresh_timer: Optional[TimerHandle] = None
        self._refresh_required: bool = kwargs.get("refresh_required", False)
        self._delay: int = self.base_delay
        self._reconnect_timer: Optional[TimerHandle] = None
        self._disconnect_event: asyncio.Event = Event()
        self._close_event: asyncio.Event = Event()
        self._connect_futures: Dict[int, ConnectFuture] = {}
        self._loop = kwargs.get("loop", asyncio.get_event_loop())
        # @see https://docs.python.org/zh-cn/3.10/library/asyncio-task.html?highlight=discard#asyncio.create_task
        self._running_tasks: Set = set()
        self._handlers: Dict[str, Callable] = {
            "on_connect": kwargs.get("on_connect"),
            "on_connected": kwargs.get("on_connected"),
            "on_server_subscribed": kwargs.get("on_subscribed"),
            "on_server_unsubscribed": kwargs.get("on_unsubscribed"),
            "on_server_subscribing": kwargs.get("on_subscribing"),
            "on_server_publication": kwargs.get("on_publication"),
            "on_disconnected": kwargs.get("on_disconnected"),
            "on_error": kwargs.get("on_error"),
            "on_message": kwargs.get("on_message"),
        }

        self._check_handlers()

    def _check_handlers(self):
        for key, handler in self._handlers.items():
            if handler and not asyncio.iscoroutinefunction(handler):
                raise ConfigurationError(f"handler {key} must be coroutine function")

    async def _move_to_disconnected(self, code: int = 0, reason: str = "no reason"):
        if self._status in (STATUS_DISCONNECTED, STATUS_CLOSED):
            return

        if self._transport:
            await asyncio.shield(self._transport.close())
            self._transport = None

        prev_status = self._status
        async with asyncio.Lock():
            self._status = STATUS_DISCONNECTED
            await self._clear_connected_state()
            await self._resolve_connect_futures(ClientDisconnected())

            server_subs_to_unsubscribe = self._server_subs.keys()

        if prev_status == STATUS_CONNECTED:
            server_subscribing_handler = self._handlers.get("on_server_subscribing")
            if server_subscribing_handler:

                async def _server_subscribing_handler():
                    for ch in server_subs_to_unsubscribe:
                        await server_subscribing_handler(
                            ServerSubscribingEvent(channel=ch)
                        )

                await self._run_handler_async(_server_subscribing_handler)

        handler = self._handlers.get("on_disconnected")
        if handler:
            event = DisconnectEvent(code=code, reason=reason)
            await self._run_handler_sync(handler, event)

    async def _move_to_connecting(self, code: int, reason: str):
        if self._status in (STATUS_CONNECTING, STATUS_CONNECTED, STATUS_CLOSED):
            return

        if self._transport:
            await asyncio.shield(self._transport.close())
            self._transport = None

        async with asyncio.Lock():
            self._status = STATUS_CONNECTING
            await self._clear_connected_state()
            await self._resolve_connect_futures(ClientDisconnected())

            server_subs_to_unsubscribe = self._server_subs.keys()

        server_subscribing_handler = self._handlers.get("on_server_subscribing")
        if server_subscribing_handler:

            async def _server_subscribing_handler():
                for ch in server_subs_to_unsubscribe:
                    await server_subscribing_handler(ServerSubscribingEvent(channel=ch))

            await self._run_handler_sync(_server_subscribing_handler)

        handler = self._handlers.get("on_connect")
        if handler:
            event = ConnectingEvent(code=code, reason=reason)
            await self._run_handler_sync(handler, event)

        if self._status != STATUS_CONNECTING:
            return

        self._delay = self._exponential_backoff(self._delay)
        self._reconnect_timer = self._set_timer(
            self._delay,
            self._run_task,
            self._start_reconnecting,
            timer=self._reconnect_timer,
        )

    async def _move_to_closed(self):
        if self._status == STATUS_CLOSED:
            return

        async with asyncio.Lock():
            self._status = STATUS_CLOSED

            server_subs_to_unsubscribe = self._server_subs.keys()

        server_unsubscribed_handler = self._handlers.get("on_server_unsubscribed")
        if server_unsubscribed_handler:

            async def _server_unsubscribed_handler():
                for ch in server_subs_to_unsubscribe:
                    await server_unsubscribed_handler(
                        ServerUnsubscribedEvent(channel=ch)
                    )

            await self._run_handler_async(_server_unsubscribed_handler)

        async with asyncio.Lock():
            await self._cb_queue.close()
            self._cb_queue = None

    async def _clear_connected_state(self):
        """
        clear connected state such as timer
        """
        if self._reconnect_timer:
            self._reconnect_timer.cancel()
            self._reconnect_timer = None

        if self._refresh_timer:
            self._refresh_timer.cancel()
            self._refresh_timer = None

        if self._close_event:
            self._close_event.clear()

        if self._disconnect_event:
            self._disconnect_event.clear()

        if self._requests:
            for req in self._requests.values():
                if req.cb:
                    self._run_task(req.cb, None, ClientDisconnected())
            self._requests = {}

    def _exponential_backoff(self, delay: int):
        delay = min(delay * self.factor, self.max_delay)
        return delay + random.randint(0, int(delay * self.jitter))

    def _run_task(self, coro, *args):
        """
        @see https://github.com/python/cpython/issues/91887
        """
        task = asyncio.create_task(coro(*args))
        self._running_tasks.add(task)
        task.add_done_callback(self._running_tasks.discard)

    def _set_timer(
        self, delay: int, callback: Callable, *args, timer: TimerHandle = None
    ):
        if timer:
            timer.cancel()

        if asyncio.iscoroutinefunction(callback):
            return self._loop.call_later(delay, asyncio.create_task, callback, *args)

        return self._loop.call_later(delay, callback, *args)

    async def _handle_refresh_error(self, e: RefreshEvent):
        if self._status != STATUS_CONNECTED:
            return

        await self._handle_error(e)
        self._refresh_timer = self._set_timer(
            10, self._run_task, self._send_refresh, timer=self._refresh_timer
        )

    async def _handle_error(self, e: ErrorEvent):
        handler = self._handlers.get("on_error")
        if handler:
            await self._run_handler_sync(handler, e)

    async def _handle_disconnect(self, disconnect: DisconnectState = None):
        if not disconnect:
            disconnect = DisconnectState(
                code=CONNECTING_TRANSPORT_CLOSED,
                reason="transport closed",
                reconnect=True,
            )
            if disconnect.reconnect:
                await self._move_to_connecting(disconnect.code, disconnect.reason)
            else:
                await self._move_to_disconnected(disconnect.code, disconnect.reason)

    async def _refresh_token(self):
        handler = self.config.get_token
        if not handler:
            raise ConfigurationError("get_token muse be set to handle expired token")
        if not asyncio.iscoroutinefunction(handler):
            raise ConfigurationError("get_token must be coroutine function")
        return await handler(ConnectionTokenEvent())

    async def _start_reconnecting(self):
        async with websockets.connect(self.endpoint) as websocket:
            try:
                if self._status != STATUS_CONNECTING:
                    return

                t = WebsocketTransport(conn=websocket)
                t.start_listener()

                refresh_required = self._refresh_required
                if refresh_required:
                    token = await self._refresh_token()
                    if token == "":
                        await self._move_to_disconnected(
                            DISCONNECTED_UNAUTHORIZED, "unauthorized"
                        )
                        return
                    async with asyncio.Lock():
                        self._token = token
                    if self._status != STATUS_CONNECTING:
                        return
                if self._status != STATUS_CONNECTING:
                    await asyncio.shield(t.close())
                    return
                self._refresh_required = False
                self._transport = t

                self._run_task(self._reader, t)

                await self._send_connect()
                async with asyncio.Lock():
                    await self._resolve_connect_futures(None)

                await self._close_event.wait()
            except WebSocketException as e:
                err_msg = "Unknown websocket error." if str(e) == "" else str(e)
                await self._handle_error(ErrorEvent(error=err_msg))
                return
            except ConnectionClosedError as e:
                await self._handle_error(ErrorEvent(error=str(e)))
            except OSError:
                err_msg = "The TCP connection fails."
                await self._handle_error(ErrorEvent(error=err_msg))
                return
            except ConfigurationError as e:
                await self._handle_error(RefreshEvent(error=str(e)))
                if self._status != STATUS_CONNECTING:
                    await asyncio.shield(t.close())
                    return

            self._delay = self._exponential_backoff(self._delay)
            self._reconnect_timer = self._set_timer(
                self._delay,
                self._run_task,
                self._start_reconnecting,
                timer=self._reconnect_timer,
            )

    async def _start_connecting(self):
        if self._status == STATUS_CLOSED:
            raise ClientClosed()

        if self._status in (STATUS_CONNECTED, STATUS_CONNECTING):
            return

        if not self._close_event:
            self._close_event = Event()

        if not self._disconnect_event:
            self._disconnect_event = Event()

        async with asyncio.Lock():
            self._status = STATUS_CONNECTING

        handler = self._handlers.get("on_connect")
        if handler:
            event = ConnectingEvent(
                code=CONNECTING_CONNECT_CALLED, reason="connect called"
            )
            await self._run_handler_sync(handler, event)

        await self._start_reconnecting()

    async def connect(self):
        """
        connect dials to server and sends connect message. Will return an error if first
        dial with a server failed. In case of failure client will automatically reconnect.
        To temporary disconnect from a server call Client.disconnect.
        """
        await self._start_connecting()

    async def disconnect(self):
        """
        disconnect client from server. It's still possible to connect again later.
        If you don't need Client anymore – use Client.close.
        """
        if self._is_closed():
            raise ClientClosed()

        await self._move_to_disconnected(
            code=DISCONNECTED_DISCONNECT_CALLED, reason="disconnect called"
        )

    async def close(self):
        """
        close closes Client and cleanups resources. Client is unusable after this.
        Use this method if you don't need client anymore, otherwise look at Client.disconnect.
        :return:
        """
        if self._is_closed():
            return

        await self._move_to_disconnected(
            code=DISCONNECTED_DISCONNECT_CALLED, reason="disconnect called"
        )
        await self._move_to_closed()

    def state(self):
        return self._status

    def _next_future_id(self):
        self._future_id += 1
        return self._future_id

    def _next_cmd_id(self):
        self._cmd_id += 1
        return self._cmd_id

    def is_connected(self):
        return self._status == STATUS_CONNECTED

    def _is_closed(self):
        return self._status == STATUS_CLOSED

    async def _send_connect(self) -> None:
        cmd = Command(id=self._next_cmd_id())
        if (
            self._token != ""
            or self._data is not None
            or self.config.name != ""
            or self.config.version != ""
        ):
            params = ConnectRequest(
                token=self._token, name=self.config.name, version=self.config.version
            )
            if self._data is not None:
                params.data = self._data

            cmd.connect = params

        await self._send_async(cmd, self._handle_connect)

    async def _send_refresh(self):
        try:
            token = await self._refresh_token()
        except ConfigurationError as e:
            await self._handle_refresh_error(RefreshEvent(error=str(e)))
            return

        self._token = token
        cmd = Command(id=self._next_cmd_id())
        params = RefreshRequest(token=token)
        cmd.refresh = params

        await self._send_async(cmd, self._handle_refresh)

    async def _send_publish(
        self,
        channel: str,
        data: bytes,
        queue: asyncio.Queue,
    ):
        cmd = Command(id=self._next_cmd_id())
        cmd.publish = PublishRequest(channel=channel, data=data)

        async def _handle_fn(reply: Optional[Reply], err: Optional[TransportError]):
            await self._handle_publish(reply, err, queue)

        await self._send_async(cmd, _handle_fn)

    async def _send_history(
        self, channel: str, opts: HistoryOptions, queue: asyncio.Queue
    ):
        params = HistoryRequest(channel=channel, limit=opts.limit, reverse=opts.reverse)
        if opts.since is not None:
            params.since = StreamPosition(
                offset=opts.since.offset, epoch=opts.since.epoch
            )

        cmd = Command(id=self._next_cmd_id())
        cmd.history = params

        async def _handle_fn(reply: Optional[Reply], err: Optional[TransportError]):
            await self._handle_history(reply, err, queue)

        await self._send_async(cmd, _handle_fn)

    async def _send_presence(self, channel: str, queue: asyncio.Queue):
        params = PresenceRequest(channel=channel)
        cmd = Command(id=self._next_cmd_id())
        cmd.presence = params

        async def _handle_fn(reply: Optional[Reply], err: Optional[TransportError]):
            await self._handle_presence(reply, err, queue)

        await self._send_async(cmd, _handle_fn)

    async def _send_presence_stats(self, channel: str, queue: asyncio.Queue):
        params = PresenceStatsRequest(channel=channel)
        cmd = Command(id=self._next_cmd_id())
        cmd.presence = params

        async def _handle_fn(reply: Optional[Reply], err: Optional[TransportError]):
            await self._handle_presence_stats(reply, err, queue)

        await self._send_async(cmd, _handle_fn)

    async def _handle_connect(
        self, reply: Optional[Reply], err: Optional[TransportError]
    ):
        if self._status != STATUS_CONNECTING:
            return

        if err:
            await self._handle_error(ErrorEvent(error=str(err)))
            await asyncio.shield(self._transport.close())
            return

        if reply and reply.error:
            if reply.error.code == 109:
                """
                token expired
                """
                if self._status != STATUS_CONNECTING:
                    return
                self._refresh_required = True

                self._delay = self._exponential_backoff(self._delay)
                self._reconnect_timer = self._set_timer(
                    self._delay,
                    self._run_task,
                    self._start_reconnecting,
                    timer=self._reconnect_timer,
                )
                return
            elif reply.error.temporary:
                """
                temporary error
                """
                if self._status != STATUS_CONNECTING:
                    return

                self._delay = self._exponential_backoff(self._delay)
                self._reconnect_timer = self._set_timer(
                    self._delay,
                    self._run_task,
                    self._start_reconnecting,
                    timer=self._reconnect_timer,
                )
                return
            else:
                """
                server error
                """
                await self._move_to_disconnected(
                    code=reply.error.code, reason=reply.error.message
                )
                return

        if self._status != STATUS_CONNECTING:
            await asyncio.shield(self._transport.close())
            return

        async with asyncio.Lock():
            self._status = STATUS_CONNECTED
        res = reply.connect
        if res.expires:
            self._refresh_timer = self._set_timer(
                res.ttl, self._run_task, self._send_refresh, timer=self._refresh_timer
            )

        handler = self._handlers.get("on_connected")
        if handler:
            event = ConnectedEvent(
                client_id=res.client, version=res.version, data=res.data
            )
            await self._run_handler_sync(handler, event)

        subscribe_handler = self._handlers.get("on_server_subscribed")
        publish_handler = self._handlers.get("on_server_publication")

        for channel, sub_res in res.subs.items():
            sub = self._server_subs.get(channel)
            if sub:
                sub.epoch = sub_res.epoch
                sub.recoverable = sub_res.recoverable
            else:
                sub = ServerSub(
                    offset=sub_res.offset,
                    epoch=sub_res.epoch,
                    recoverable=sub_res.recoverable,
                )
            if not sub_res.publications or len(sub_res.publications) == 0:
                sub.offset = sub_res.offset
            async with asyncio.Lock():
                self._server_subs[channel] = sub

            if subscribe_handler:
                event = ServerSubscribedEvent(
                    channel=channel,
                    data=sub_res.data,
                    recovered=sub_res.recovered,
                    was_recovering=sub_res.was_recovering,
                    positioned=sub_res.positioned,
                    recoverable=sub_res.recoverable,
                )
                if event.positioned or event.recoverable:
                    event.stream_position = StreamPosition(
                        epoch=sub_res.epoch, offset=sub_res.offset
                    )
                await self._run_handler_sync(subscribe_handler, event)

            if publish_handler:

                async def _publish_handler():
                    for pub in sub_res.publications:
                        server_sub = self._server_subs.get(channel)
                        if server_sub:
                            server_sub.offset = pub.offset
                        async with asyncio.Lock():
                            self._server_subs[channel] = sub

                        self._run_task(
                            _publish_handler,
                            ServerPublicationEvent(channel=channel, **pub.model_dump()),
                        )

                await self._run_handler_sync(_publish_handler)

        for ch in self._server_subs:
            if not res.subs.get(ch):
                server_unsubscribed_handler = self._handlers.get(
                    "on_server_unsubscribed"
                )
                if server_unsubscribed_handler:
                    await self._run_handler_sync(
                        server_unsubscribed_handler, ServerUnsubscribedEvent(channel=ch)
                    )

        self._delay = self.base_delay

        if self._status != STATUS_CONNECTED:
            return

        if res.ping and int(res.ping) > 0:
            self._send_pong = res.pong
            self._run_task(self._wait_server_ping, res.ping)

    async def _handle_refresh(
        self, reply: Optional[Reply], err: Optional[TransportError]
    ):
        if err:
            await self._handle_refresh_error(RefreshEvent(error=str(err)))
            return
        if not reply:
            return

        if reply.error:
            if self._status != STATUS_CONNECTED:
                return

            if reply.error.temporary:
                await self._handle_refresh_error(RefreshEvent(error=reply.error))
            else:
                await self._move_to_disconnected(
                    code=reply.error.code, reason=reply.error.message
                )
            return
        expires = reply.refresh.expires
        ttl = reply.refresh.ttl
        if expires:
            if self._status != STATUS_CONNECTED:
                return
            self._refresh_timer = self._set_timer(
                ttl, self._run_task, self._send_refresh, timer=self._refresh_timer
            )

    async def _handle_publish(
        self,
        reply: Optional[Reply],
        err: Optional[TransportError],
        queue: asyncio.Queue,
    ):
        publish_result = PublishResult().model_dump_json()
        if err:
            await queue.put((publish_result, err))
            return
        if reply.error:
            await queue.put(
                (publish_result, CentrifugeServerError(**reply.error.model_dump()))
            )
            return
        await queue.put((publish_result, None))

    async def _handle_history(
        self,
        reply: Optional[Reply],
        err: Optional[TransportError],
        queue: asyncio.Queue,
    ):
        history_result = HistoryResult().model_dump_json()
        if err:
            await queue.put((history_result, err))
            return
        if reply.error:
            await queue.put(
                (history_result, CentrifugeServerError(**reply.error.model_dump()))
            )
            return
        await queue.put((reply.history.model_dump_json(), None))

    async def _handle_presence(
        self,
        reply: Optional[Reply],
        err: Optional[TransportError],
        queue: asyncio.Queue,
    ):
        presence_result = PresenceResult().model_dump_json()
        if err:
            await queue.put((presence_result, err))
            return
        if reply.error:
            await queue.put(
                (presence_result, CentrifugeServerError(**reply.error.model_dump()))
            )
            return
        await queue.put((reply.presence.model_dump_json(), None))

    async def _handle_presence_stats(
        self,
        reply: Optional[Reply],
        err: Optional[TransportError],
        queue: asyncio.Queue,
    ):
        presence_stats_result = PresenceStatsResult().model_dump_json()
        if err:
            await queue.put((presence_stats_result, err))
            return
        if reply.error:
            await queue.put(
                (
                    presence_stats_result,
                    CentrifugeServerError(**reply.error.model_dump()),
                )
            )
            return
        await queue.put((reply.presence_stats.model_dump_json(), None))

    async def _wait_server_ping(self, ping_interval: int):
        timeout = self.config.max_server_ping_delay + ping_interval
        while not self._close_event.is_set():
            await asyncio.wait(
                [self._disconnect_event.wait(), self._delay_ping.wait()],
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self._disconnect_event.is_set():
                self._disconnect_event.clear()
                return

            if self._delay_ping.is_set():
                self._delay_ping.clear()
                continue
            else:
                self._run_task(
                    self._handle_disconnect,
                    DisconnectState(
                        code=CONNECTING_NO_PING, reason="no ping", reconnect=True
                    ),
                )
                return

    async def _add_request(self, req_id: int, cb: Callable):
        async with asyncio.Lock():
            self._requests[req_id] = Request(cb=cb)

    async def _remove_request(self, req_id: int):
        async with asyncio.Lock():
            if self._requests.get(req_id):
                del self._requests[req_id]

    async def _read_once(self, t: WebsocketTransport) -> (Reply, DisconnectState):
        disconnect = None
        try:
            reply, disconnect = await t.read()
        except WebSocketException as e:
            await self._handle_disconnect(
                disconnect=DisconnectState(
                    code=CONNECTING_TRANSPORT_CLOSED, reason=str(e)
                )
            )
            return None, disconnect

        if reply is None:
            await self._handle_disconnect(disconnect=disconnect)
            return None, disconnect
        await self._handle_reply(reply)
        return reply, None

    async def _handle_reply(self, reply: Reply):
        if reply.id and int(reply.id) > 0:
            req = self._requests.get(reply.id)
            if req and req.cb:
                self._run_task(req.cb, reply, None)
            await self._remove_request(reply.id)
        else:
            if reply.push is None:
                # Ping from server, send pong if needed
                self._delay_ping.set()
                if self._send_pong:
                    await self._send(Command())
                return
            if self._status != STATUS_CONNECTED:
                return
            await self._handle_push(reply.push)

    async def _handle_push(self, push: Push):
        if push.pub is not None:
            channel = push.channel
            publish_handler = self._handlers.get("on_server_publication")
            if publish_handler:
                await self._run_handler_sync(
                    publish_handler,
                    ServerPublicationEvent(channel=channel, **push.pub.model_dump()),
                )

    async def _reader(self, t: WebsocketTransport):
        while True:
            _, disconnect = await self._read_once(t)
            if disconnect:
                break
        self._disconnect_event.set()

    async def _run_handler_sync(
        self, fn: Callable[[CentrifugeEvent], Awaitable], e: CentrifugeEvent = None
    ):
        wait_event = Event()
        wrapped = (
            wrapper_with_event(fn, e, wait_event)
            if e
            else wrapper_without_event(fn, wait_event)
        )
        await self._cb_queue.push(wrapped)
        await wait_event.wait()

    async def _run_handler_async(
        self, fn: Callable[[CentrifugeEvent], Awaitable], e: CentrifugeEvent = None
    ):
        wrapped = wrapper_with_event(fn, e) if e else wrapper_without_event(fn)
        await self._cb_queue.push(wrapped)

    async def _send(self, cmd: Command):
        transport = self._transport
        if transport is None:
            return ClientDisconnected()
        try:
            await self._transport.write(cmd, timeout=self.config.write_timeout)
        except CancelledError:
            self._run_task(
                self._handle_disconnect,
                DisconnectState(
                    code=CONNECTING_TRANSPORT_CLOSED,
                    reason="write error",
                    reconnect=True,
                ),
            )
        except TimeoutError:
            self._run_task(
                self._handle_disconnect,
                DisconnectState(
                    code=CONNECTING_TRANSPORT_CLOSED,
                    reason="write error",
                    reconnect=True,
                ),
            )

    async def _send_async(self, cmd: Command, cb: Callable) -> None:
        await self._add_request(cmd.id, cb)
        await self._send(cmd)

        try:
            await asyncio.wait_for(
                self._close_event.wait(), timeout=self.config.read_timeout
            )
        except asyncio.TimeoutError:
            req = self._requests.get(cmd.id)
            if not req:
                return
            self._run_task(req.cb, None, Timeout())
        finally:
            if self._close_event.is_set():
                self._close_event.clear()
                req = self._requests.get(cmd.id)
                if not req:
                    return
                self._run_task(req.cb, None, ClientDisconnected())

            await self._remove_request(cmd.id)

    async def _resolve_connect_futures(self, err: Optional[TransportError] = None):
        for future in self._connect_futures.values():
            self._run_task(future.fn, None, err)
        self._connect_futures = {}

    async def _on_connect(self, fn: Callable[[Optional[TransportError]], Awaitable]):
        if self._status == STATUS_CONNECTED:
            await fn(None)
            return
        if self._status == STATUS_DISCONNECTED:
            await fn(ClientDisconnected())
            return

        fut_id = self._next_future_id()
        fut = ConnectFuture(fn=fn, close_event=Event())
        async with asyncio.Lock():
            self._connect_futures[fut_id] = fut

        try:
            await asyncio.wait_for(
                fut.close_event.wait(), timeout=self.config.read_timeout
            )
        except asyncio.TimeoutError:
            fut = self._connect_futures.get(fut_id)
            if not fut:
                return
            async with asyncio.Lock():
                del self._connect_futures[fut_id]
            await fut.fn(Timeout())

    async def publish(self, channel: str, data: str) -> PublishResult:
        """
        publish data to channel
        """
        if self._is_closed():
            raise ClientClosed()

        queue = asyncio.Queue()
        self._run_task(self._publish, channel, data.encode("utf-8"), queue)

        result, err = await queue.get()
        if err:
            raise err
        return result

    async def _publish(self, channel: str, data: bytes, queue: asyncio.Queue):
        async def _send_data(err: TransportError = None):
            if err:
                await self._handle_publish(Reply(publish=PublishResult()), err, queue)
            else:
                await self._send_publish(channel, data, queue)

        await self._on_connect(_send_data)

    async def history(self, channel: str, opts: HistoryOptions) -> HistoryResult:
        if self._is_closed():
            raise ClientClosed()

        queue = asyncio.Queue()
        self._run_task(self._history, channel, opts, queue)

        result, err = await queue.get()
        if err:
            raise err
        return result

    async def _history(self, channel: str, opts: HistoryOptions, queue: asyncio.Queue):
        async def _send_data(err: TransportError = None):
            if err:
                await self._handle_history(Reply(history=HistoryResult()), err, queue)
            else:
                await self._send_history(channel, opts, queue)

        await self._on_connect(_send_data)

    async def presence(self, channel: str) -> PresenceResult:
        if self._is_closed():
            raise ClientClosed()

        queue = asyncio.Queue()
        self._run_task(self._presence, channel, queue)

        result, err = await queue.get()
        if err:
            raise err
        return result

    async def _presence(self, channel: str, queue: asyncio.Queue):
        async def _send_data(err: TransportError = None):
            if err:
                await self._handle_presence(
                    Reply(presence=PresenceResult()), err, queue
                )
            else:
                await self._send_presence(channel, queue)

        await self._on_connect(_send_data)

    async def presence_stats(self, channel: str) -> PresenceResult:
        if self._is_closed():
            raise ClientClosed()

        queue = asyncio.Queue()
        self._run_task(self._presence_stats, channel, queue)

        result, err = await queue.get()
        if err:
            raise err
        return result

    async def _presence_stats(self, channel: str, queue: asyncio.Queue):
        async def _send_data(err: TransportError = None):
            if err:
                await self._handle_presence_stats(
                    Reply(presence_stats=PresenceStatsResult()), err, queue
                )
            else:
                await self._send_presence_stats(channel, queue)

        await self._on_connect(_send_data)


def wrapper_with_event(
    fn: Callable[[CentrifugeEvent], Awaitable],
    e: CentrifugeEvent = None,
    wait_event: Event = None,
):
    async def _wrapper():
        if wait_event:
            wait_event.set()
        return await fn(e)

    return _wrapper


def wrapper_without_event(
    fn: Callable[[], Awaitable],
    wait_event: Event = None,
):
    async def _wrapper():
        if wait_event:
            wait_event.set()
        return await fn()

    return _wrapper


class JsonClient(Client):
    """
    JsonClient initializes Client which uses JSON-based protocol internally.
    After client initialized call Client.Connect method.
    Use Client.new_subscription to create Subscription objects.
    """

    pass
