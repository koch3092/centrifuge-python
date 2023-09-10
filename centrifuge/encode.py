from abc import ABC, abstractmethod
from typing import List

from centrifuge.models.command import Command
from centrifuge.models.reply import Reply


class CommandEncoder(ABC):
    @abstractmethod
    def encode(self, commands: List[Command]) -> bytes:
        pass


class JSONCommandEncoder(CommandEncoder):
    def encode(self, commands: List[Command]) -> bytes:
        return "\n".join(
            [
                command.model_dump_json(
                    exclude_none=True, exclude_unset=True, exclude_defaults=True
                )
                for command in commands
            ]
        ).encode("utf-8")


class ReplyDecoder(ABC):
    @abstractmethod
    def decode(self, message: bytes) -> List[Reply]:
        pass


class JSONReplyDecoder(ReplyDecoder):
    def decode(self, message: bytes) -> List[Reply]:
        return [Reply.model_validate_json(reply) for reply in message.split("\n")]
