from typing import Type

from typing_extensions import Protocol


class Manifest(Protocol):
    def get_id(self) -> str:
        ...

    @classmethod
    def parse_dict(cls: Type["Manifest"], d: dict) -> "Manifest":
        ...
