from typing import Type

from typing_extensions import Protocol


class State(Protocol):
    def is_updated(self) -> bool:
        ...


class Manifest(Protocol):
    def get_id(self) -> str:
        ...

    def get_kind(self) -> str:
        ...

    @classmethod
    def parse_dict(cls: Type["Manifest"], d: dict) -> "Manifest":
        ...
