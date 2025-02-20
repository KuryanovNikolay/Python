import itertools
import typing as tp
from abc import abstractmethod, ABC

TKey = tp.Tuple[tp.Any, ...]
TRow = dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Operation(ABC):
    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


class Joiner(ABC):  # type ignore
    """Base class for joiners"""

    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:  # type ignore
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b

    @abstractmethod
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:  # type ignore
        """
        :param keys: join keys
        :param rows_a: left table rows
        :param rows_b: right table rows
        """
        pass


class Join(Operation):  # type ignore
    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):  # type ignore
        self.keys = keys
        self.joiner = joiner

    def _keyfunc(self, row: TRow) -> TKey:  # type ignore
        """Generate a tuple key for grouping rows by specified keys."""
        return tuple(row[key] for key in self.keys)

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:  # type ignore
        rows_b = args[0] if args else []

        iter_a = itertools.groupby(rows, key=self._keyfunc)
        iter_b = itertools.groupby(rows_b, key=self._keyfunc)
        key_a, group_a = next(iter_a, (None, None))
        key_b, group_b = next(iter_b, (None, None))
        while key_a is not None or key_b is not None:
            if key_a == key_b:
                yield from self.joiner(self.keys, group_a, group_b)  # type: ignore[arg-type]
                key_a, group_a = next(iter_a, (None, None))
                key_b, group_b = next(iter_b, (None, None))
            elif key_b is None or (key_a is not None and key_a < key_b):
                if isinstance(self.joiner, (LeftJoiner, OuterJoiner)):
                    yield from self.joiner(self.keys, group_a, [])  # type: ignore[arg-type]
                key_a, group_a = next(iter_a, (None, None))
            else:
                if isinstance(self.joiner, (RightJoiner, OuterJoiner)):
                    yield from self.joiner(self.keys, [], group_b)  # type: ignore[arg-type]
                key_b, group_b = next(iter_b, (None, None))


class InnerJoiner(Joiner):
    """Join with inner strategy"""

    def __call__(
            self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable
    ) -> TRowsGenerator:
        rows_b_list = list(rows_b)
        for row_a in rows_a:
            for row_b in rows_b_list:
                yield self._merge_rows(row_a, row_b, keys)

    def _merge_rows(self, row_a: TRow, row_b: TRow, keys: tp.Sequence[str]) -> TRow:
        result = {}
        for key in keys:
            result[key] = row_a[key]
        columns_a = set(row_a.keys()) - set(keys)
        columns_b = set(row_b.keys()) - set(keys)
        for k in columns_a:
            if k in columns_b:
                result[k + self._a_suffix] = row_a[k]
            else:
                result[k] = row_a[k]
        for k in columns_b:
            if k in columns_a:
                result[k + self._b_suffix] = row_b[k]
            else:
                result[k] = row_b[k]
        return result


class OuterJoiner(Joiner):
    """Join with outer strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_a_list = list(rows_a)
        rows_b_list = list(rows_b)
        matched_b = set()

        for row_a in rows_a_list:
            matched = False
            for row_b in rows_b_list:
                if all(row_a[key] == row_b[key] for key in keys):
                    yield {**row_a, **row_b}
                    matched_b.add(id(row_b))
                    matched = True
            if not matched:
                yield row_a

        for row_b in rows_b_list:
            if id(row_b) not in matched_b:
                yield row_b


class LeftJoiner(Joiner):
    """Join with left strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_b_list = list(rows_b)
        for row_a in rows_a:
            matched = False
            for row_b in rows_b_list:
                if all(row_a[key] == row_b[key] for key in keys):
                    yield {**row_a, **row_b}
                    matched = True
            if not matched:
                yield row_a


class RightJoiner(Joiner):
    """Join with right strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_a_list = list(rows_a)
        for row_b in rows_b:
            matched = False
            for row_a in rows_a_list:
                if all(row_b[key] == row_a[key] for key in keys):
                    yield {**row_a, **row_b}
                    matched = True
            if not matched:
                yield row_b
