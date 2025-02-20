from abc import abstractmethod, ABC
from math import log, radians, asin, sin, pow, sqrt, cos  # noqa: F401
import typing as tp  # noqa: F401
from .mappers import (Mapper, Project, Filter, Product, Split, LowerCase,  # noqa: F401
                      FilterPunctuation, DummyMapper, Map, Calculate, ReverseFreq)  # noqa: F401
from .reducers import Average, Sum, Count, TermFrequency, TopN, FirstReducer, Reduce, Reducer  # noqa: F401
from .joiners import RightJoiner, LeftJoiner, OuterJoiner, InnerJoiner, Join, Joiner  # noqa: F401
from .mappers import haversine_distance, road_time, hour, weekday, speed  # noqa: F401

TRow = dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Operation(ABC):
    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


class Read(Operation):
    def __init__(self, filename: str, parser: tp.Callable[[str], TRow]) -> None:
        self.filename = filename
        self.parser = parser

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        with open(self.filename) as f:
            for line in f:
                yield self.parser(line)


class ReadIterFactory(Operation):
    def __init__(self, name: str) -> None:
        self.name = name

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in kwargs[self.name]():
            yield row
