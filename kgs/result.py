from dataclasses import dataclass
from enum import Enum
from typing import Generic
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypeVar

T = TypeVar("T")


class ResultKind(Enum):
    unknown = "unknown"
    notfound = "notfound"


@dataclass
class Result(Generic[T]):
    result: Optional[T]
    detail: Optional[dict]
    is_err: bool
    kind: ResultKind = ResultKind.unknown

    @staticmethod
    def ok(result: T, detail: Optional[dict] = None) -> "Result[T]":
        return Result[T](result, detail, False)

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
