from pathlib import Path
from typing import List, TypeVar, Type
from typing_extensions import Protocol


T = TypeVar('T', bound='Manifest')


class Manifest(Protocol):
    def get_id(self) -> str:
        ...

    def get_filepath(self) -> List[str]:
        ...

    @classmethod
    def load(cls: Type[T], path_list: List[Path]) -> List[T]:
        ...
