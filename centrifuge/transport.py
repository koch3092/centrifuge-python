import asyncio
import json
from abc import ABC, abstractmethod
from asyncio import Event, Queue
from typing import Set

import websockets
from pydantic import Field
from websockets.exceptions import ConnectionClosedError

from centrifuge.codes import (
    CONNECTING_TRANSPORT_CLOSED,
    DISCONNECT_BAD_PROTOCOL,
    DISCONNECT_MESSAGE_SIZE_LIMIT,
)
from centrifuge.encode import (
    CommandEncoder,
    JSONCommandEncoder,
    JSONReplyDecoder,
    ReplyDecoder,
)
from centrifuge.models.command import Command
from centrifuge.models.reply import Reply
from centrifuge.models.transport import DisconnectState


class Transport(ABC):
    @abstractmethod
    async def read(self) -> Reply:
        """
        Coroutine to read new Reply messages from the connection.

        This coroutine should be implemented for reading messages.
        It returns a tuple containing (Reply, Exception).
        - If a Reply is received, it is returned.
        - If an Exception is encountered during reading, it is returned.
        """
        pass

    @abstractmethod
    async def write(self, cmd: Command, timeout: int = None):
        """
        Coroutine to write a Command to the connection within the specified write timeout.

        This coroutine should be implemented for writing commands.
        It returns an Exception if an error occurs during writing.
        """
        pass

    @abstractmethod
    async def close(self):
        """
        Coroutine to close the connection and perform any required clean-up.

        This coroutine should be implemented to gracefully close the connection.
        It must be safe to call Close several times and concurrently with the Read and Write coroutines.
        """
        pass


class WebsocketTransport(Transport):
    def __init__(
        self,
        conn: websockets.WebSocketClientProtocol,
        **kwargs,
    ):
        self._conn: websockets.WebSocketClientProtocol = conn
        self._command_encoder: CommandEncoder = kwargs.get(
            "command_encoder", JSONCommandEncoder()
        )
        self._reply_decoder: ReplyDecoder = kwargs.get(
            "reply_decoder", JSONReplyDecoder()
        )
        self._reply_msg: Queue = kwargs.get("reply_msg", Queue())
        self._disconnect: DisconnectState = Field(None)
        self._closed: bool = kwargs.get("closed", False)
        self._close_event: asyncio.Event = kwargs.get("close_event", Event())

        # @see https://docs.python.org/zh-cn/3.10/library/asyncio-task.html?highlight=discard#asyncio.create_task
        self._background_tasks: Set = set()

    def _run_task(self, coro):
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def start_listener(self):
        self._run_task(self._reader())

    async def read(self) -> (Reply, DisconnectState):
        reply: Reply = await self._reply_msg.get()
        self._reply_msg.task_done()
        if reply is None:
            return None, self._disconnect
        return reply, None

    async def write(self, cmd: Command, timeout: int = None):
        data = self._command_encoder.encode([cmd])
        await self._write_data(data, timeout)

    async def close(self):
        if self._closed:
            return
        self._closed = True
        self._close_event.set()
        await self._reply_msg.put(None)
        await self._conn.close()

    async def _reader(self):
        try:
            async for message in self._conn:
                replies = self._reply_decoder.decode(message)
                for reply in replies:
                    await self._reply_msg.put(reply)
        except ValueError:
            async with asyncio.Lock():
                self._disconnect = DisconnectState(
                    reconnect=False,
                    code=DISCONNECT_BAD_PROTOCOL,
                    reason="decode error",
                )
        except ConnectionClosedError as e:
            async with asyncio.Lock():
                self._disconnect = extract_disconnect_websocket(e)
        finally:
            await asyncio.shield(self.close())

    async def _write_data(self, data: bytes, timeout: int = 0):
        await asyncio.wait_for(self._conn.send(data), timeout=timeout)


def extract_disconnect_websocket(e: ConnectionClosedError) -> DisconnectState:
    try:
        d = json.loads(e.reason)
    except ValueError:
        code = e.code
        reason = e.reason
        reconnect = code < 3500 or code >= 5000 or (4000 <= code < 4500)
        if code < 3000:
            if code == 1009:
                code = DISCONNECT_MESSAGE_SIZE_LIMIT
            else:
                # We expose codes defined by Centrifuge protocol, hiding details about transport-specific error codes.
                # We may have extra optional transportCode field in the future.
                code = CONNECTING_TRANSPORT_CLOSED

        d = DisconnectState(code=code, reason=reason, reconnect=reconnect)

    return d
