from typing import Type
from typing import TypeVar

from typing_extensions import Protocol


ManifestLike = TypeVar("ManifestLike", bound="Manifest")


class Manifest(Protocol):
    def get_id(self) -> str:
        ...

    @classmethod
    def parse_dict(cls: Type[ManifestLike], d: dict) -> ManifestLike:
        ...


class State(Protocol):
    def is_updated(self, m: Type["Manifest"]) -> bool:
        ...
