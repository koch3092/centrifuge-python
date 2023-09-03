class CentrifugeException(Exception):
    """
    CentrifugeException is a base exception for all other exceptions
    in this library.
    """

    pass


class TransportError(CentrifugeException):
    """
    TransportError raised when websocket transport error occurs.
    """

    pass


class ConfigurationError(CentrifugeException):
    """
    ConfigurationError raised when library configured incorrectly.
    """

    pass


class ClientClosed(TransportError):
    """
    ClientClosed can be returned if client is closed.
    """

    def __init__(self):
        super().__init__("client closed")


class ClientDisconnected(TransportError):
    """
    ClientDisconnected can be returned if client goes to disconnected state while operation in progress.
    """

    def __init__(self):
        super().__init__("client disconnected")


class Timeout(TransportError):
    """
    Timeout returned if operation timed out.
    """

    def __init__(self):
        super().__init__("timeout")


class WebsocketConnectError(CentrifugeException):
    """
    WebsocketConnectError raised when websocket connection failed.
    """

    pass


class UnauthorizedError(CentrifugeException):
    """
    UnauthorizedError raised by application from get_token function to indicate lack of operation permission.
    """

    pass
