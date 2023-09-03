from typing import Callable, List

import websockets.datastructures


class Config:
    def __init__(self, token: str, get_token: Callable = None, **kwargs):
        """
        :param token: token for a connection authentication.
        :param get_token: get_token called to get or refresh connection token.
        :param data: data is an arbitrary data which can be sent to a server in a Connect command. Make sure it's a
        valid JSON when using JSON protocol client.
        :param name: name allows setting client name. You should only use a limited amount of client names throughout
        your applications – i.e. don't make it unique per user for example, this name semantically represents an
        environment from which client connects. Default value means "python".
        :param version: version allows setting client version. This is an application specific information. By default,
        no version set.
        :param read_timeout: read_timeout is how long to wait read operations to complete. By default, 5 seconds.
        :param write_timeout: write_timeout is Websocket write timeout. By default, 1 second.
        :param max_server_ping_delay: max_server_ping_delay specifies the duration for the handshake to complete.
        By default, 10 seconds.
        :param headers: headers specifies custom HTTP Header to send.
        """
        self.token = str(token)
        self.get_token = get_token
        self.data: List[bytes] = kwargs.get("data", None)
        self.name: str = kwargs.get("name", "python")
        self.version: str = kwargs.get("version", "")
        self.read_timeout: int = kwargs.get("read_timeout", 5)
        self.write_timeout: int = kwargs.get("write_timeout", 1)
        self.max_server_ping_delay: int = kwargs.get("max_server_ping_delay", 10)
        self.headers: websockets.datastructures.Headers = kwargs.get("headers", {})
