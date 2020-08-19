from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from typing import Generic
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type
from typing import TypeVar

from dataclasses_json import config
from dataclasses_json import dataclass_json

T = TypeVar("T")


class ResultKind(Enum):
    unknown = "unknown"
    success = "success"
    notfound = "notfound"
    updated = "updated"
    dryrun = "dryrun"

    def __str__(self):
        return str(self.value)

    @classmethod
    def from_str(cls: Type["ResultKind"], s: str) -> "ResultKind":
        return ResultKind(s)


@dataclass_json
@dataclass
class Result(Generic[T]):
    result: Optional[T]
    detail: Optional[dict]
    is_err: bool
    kind: ResultKind = field(default=ResultKind.unknown, metadata=config(encoder=ResultKind.__str__))

    @staticmethod
    def ok(result: T, detail: Optional[dict] = None, kind: ResultKind = ResultKind.success) -> "Result[T]":
        return Result[T](result, detail, False, kind)

    @staticmethod
    def err(detail: dict, kind: ResultKind = ResultKind.unknown) -> "Result[T]":
        return Result[T](None, detail, True, kind)

    @staticmethod
    def chain(result: "Result[T]"):
        return Result[T](None, result.detail, result.is_err, result.kind)

    # unwrap result and check err
    def chk(self, *args: ResultKind) -> Tuple[T, "Result[T]", List[bool]]:
        kind_chk_result = [self.kind == k for k in args]
        return self.result, self, [self.is_err and not any(kind_chk_result)] + kind_chk_result  # type:ignore
