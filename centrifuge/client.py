import asyncio
import random
from asyncio import CancelledError, Event, Queue, TimerHandle
from typing import Callable, Dict, List, Optional, Set

import websockets
from pydantic import AnyUrl
from websockets.exceptions import (
    ConnectionClosedError,
    InvalidHandshake,
    InvalidURI,
    WebSocketException,
)

from centrifuge.codes import (
    CONNECTING_CONNECT_CALLED,
    CONNECTING_NO_PING,
    CONNECTING_TRANSPORT_CLOSED,
    DISCONNECTED_DISCONNECT_CALLED,
    DISCONNECTED_UNAUTHORIZED,
)
from centrifuge.config import Config
from centrifuge.models.command import Command, ConnectRequest, RefreshRequest
from centrifuge.models.errors import (
    ClientClosed,
    ClientDisconnected,
    ConfigurationError,
    Timeout,
    TransportError,
    UnauthorizedError,
)
from centrifuge.models.events import (
    CentrifugeEvent,
    ConnectedEvent,
    ConnectingEvent,
    DisconnectEvent,
    ErrorEvent,
    RefreshEvent,
)
from centrifuge.models.reply import Reply, Request
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
        self.endpoint: AnyUrl = endpoint
        self.config: Config = config
        self._token: str = self.config.token
        self._data: List[bytes] = self.config.data
        self._status: str = kwargs.get("status", STATUS_DISCONNECTED)
        self._cmd_id = 0
        self._delay_ping: Event = kwargs.get("delay_ping", Event())
        self._send_pong: bool = kwargs.get("send_pong", False)
        self._requests: Dict[int, Request] = kwargs.get("requests", {})
        self._cb_queue: CBQueue = kwargs.get("cb_queue", CBQueue())
        self._transport: Optional[WebsocketTransport] = None
        self._refresh_timer: Optional[TimerHandle] = None
        self._refresh_required: bool = kwargs.get("refresh_required", False)
        self._delay: int = self.base_delay
        self._reconnect_timer: Optional[TimerHandle] = None
        self._disconnect_event: asyncio.Event = kwargs.get("disconnect_event", Event())
        self._disconnect: asyncio.Queue = kwargs.get("disconnect", Queue(1))
        self._close_event: asyncio.Event = kwargs.get("close_event", Event())
        self._loop = kwargs.get("loop", asyncio.get_event_loop())
        # @see https://docs.python.org/zh-cn/3.10/library/asyncio-task.html?highlight=discard#asyncio.create_task
        self._running_tasks: Set = set()
        self._handlers: Dict[str, Callable] = {
            "on_connect": kwargs.get("on_connect"),
            "on_connected": kwargs.get("on_connected"),
            "on_disconnected": kwargs.get("on_disconnected"),
            "on_error": kwargs.get("on_error"),
            "on_message": kwargs.get("on_message"),
        }

        self._pong_count = 0

        self._check_handlers()

    def _check_handlers(self):
        for key, handler in self._handlers.items():
            if handler and not asyncio.iscoroutinefunction(handler):
                raise ConfigurationError(f"handler {key} must be coroutine function")

    async def _move_to_disconnected(self, code: int = 0, reason: str = "no reason"):
        if self._status in (STATUS_DISCONNECTED, STATUS_CLOSED):
            return

        if self._transport:
            await self._transport.close()
            self._transport = None

        prev_status = self._status
        self._status = STATUS_DISCONNECTED
        await self.clear_connected_state()

        if prev_status == STATUS_CONNECTED:
            pass

        handler = self._handlers.get("on_disconnected")
        if handler:
            event = DisconnectEvent(code=code, reason=reason)
            await self._run_handler(handler, event)

    async def _move_to_connecting(self, code: int, reason: str):
        if self._status in (STATUS_CONNECTING, STATUS_CONNECTED, STATUS_CLOSED):
            return

        if self._transport:
            await self._transport.close()
            self._transport = None

        self._status = STATUS_CONNECTING
        await self.clear_connected_state()

        handler = self._handlers.get("on_connect")
        if handler:
            event = ConnectingEvent(code=code, reason=reason)
            await self._run_handler(handler, event)

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

        self._status = STATUS_CLOSED

        self._cb_queue.close()
        self._cb_queue = None

    async def clear_connected_state(self):
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

        if self._requests:
            for req in self._requests.values():
                if req.cb:
                    await req.cb(None, ClientDisconnected)  # noqa

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
            await self._run_handler(handler, e)

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
            raise UnauthorizedError("get_token muse be set to handle expired token")
        return handler()

    async def _start_reconnecting(self):
        async for websocket in websockets.connect(self.endpoint):
            try:
                if self._status != STATUS_CONNECTING:
                    return

                t = WebsocketTransport(
                    conn=websocket,
                    disconnect=self._disconnect,
                    close_event=self._close_event,
                )
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
                    await t.close()
                    return
                self._refresh_required = False
                self._transport = t

                self._run_task(self._reader, t, self._disconnect_event)

                await self._send_connect()

                await self._close_event.wait()
            except WebSocketException as e:
                err_msg = "Unknown websocket error."
                if isinstance(e, InvalidURI):
                    err_msg = "The address isn’t a valid WebSocket URI."
                elif isinstance(e, InvalidHandshake):
                    err_msg = "The opening handshake fails."
                await self._handle_error(ErrorEvent(error=err_msg))
                return
            except ConnectionClosedError:
                err_msg = "No pong received from server."
                await self._handle_error(ErrorEvent(error=err_msg))
                continue
            except OSError:
                err_msg = "The TCP connection fails."
                await self._handle_error(ErrorEvent(error=err_msg))
                return
            except UnauthorizedError:
                if self._status != STATUS_CONNECTING:
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
            await self._run_handler(handler, event)

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
        if self.is_close():
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
        if self.is_close():
            return

        await self._move_to_disconnected(
            code=DISCONNECTED_DISCONNECT_CALLED, reason="disconnect called"
        )
        await self._move_to_closed()

    def state(self):
        return self._status

    def _next_cmd_id(self):
        self._cmd_id += 1
        return self._cmd_id

    def is_connected(self):
        return self._status == STATUS_CONNECTED

    def is_close(self):
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
        except UnauthorizedError as e:
            await self._handle_refresh_error(RefreshEvent(error=e))
            return

        self._token = token
        cmd = Command(id=self._next_cmd_id())
        params = RefreshRequest(token=token)
        cmd.refresh = params

        await self._send_async(cmd, self._handle_refresh)

    async def _handle_connect(
        self, reply: Optional[Reply], err: Optional[TransportError]
    ):
        if self._status != STATUS_CONNECTING:
            return

        if err:
            await self._handle_error(ErrorEvent(error=str(err)))
            await self._transport.close()
            return

        if reply and reply.error:
            if reply.error.code == 109:
                """
                token expired
                """
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
                await self._move_to_disconnected(
                    code=reply.error.code, reason=reply.error.message
                )
                return
            else:
                """
                server error
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

        if self._status != STATUS_CONNECTING:
            return

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
            await self._run_handler(handler, event)

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

    async def _wait_server_ping(self, ping_interval: int):
        timeout = self.config.max_server_ping_delay + ping_interval
        while not self._close_event.is_set():
            await asyncio.wait(
                [self._disconnect_event.wait(), self._delay_ping.wait()],
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self._disconnect_event.is_set():
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
            del self._requests[req_id]

    async def _read_once(self, t: WebsocketTransport):
        try:
            reply, disconnect = await t.read()
        except WebSocketException as e:
            await self._handle_disconnect(
                disconnect=DisconnectState(
                    code=CONNECTING_TRANSPORT_CLOSED, reason=str(e)
                )
            )
            return

        if reply is None:
            await self._handle_disconnect(disconnect=disconnect)
            return
        await self._handle_reply(reply)

    async def _handle_reply(self, reply: Reply):
        if reply.id and int(reply.id) > 0:
            req = self._requests.get(reply.id)
            if req and req.cb:
                await req.cb(reply, None)  # noqa
            await self._remove_request(reply.id)
        else:
            if reply.push is None:
                # Ping from server, send pong if needed
                self._delay_ping.set()
                if self._send_pong:
                    self._pong_count += 1
                    await self._send(Command())
                return
            if self._status != STATUS_CONNECTED:
                return

    async def _reader(self, t: WebsocketTransport, disconnect_event: Event):
        while True:
            try:
                await self._read_once(t)
            except Exception:
                disconnect_event.set()
                break

    async def _run_handler(self, fn: Callable, e: CentrifugeEvent = None):
        async def _async_handler():
            return await fn(e)

        await self._cb_queue.push(_async_handler)

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
        done, pending = await asyncio.wait(
            [self._close_event.wait()], timeout=self.config.read_timeout
        )

        req = self._requests.get(cmd.id)
        if not req:
            return

        if self._close_event.wait in done:
            self._close_event.clear()
            await req.cb(None, ClientDisconnected())  # noqa
            return

        await req.cb(None, Timeout())  # noqa


class JsonClient(Client):
    """
    JsonClient initializes Client which uses JSON-based protocol internally.
    After client initialized call Client.Connect method.
    Use Client.new_subscription to create Subscription objects.
    """

    pass
