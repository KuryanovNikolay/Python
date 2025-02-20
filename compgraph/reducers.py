import heapq
import itertools
import typing as tp
from abc import abstractmethod, ABC
from collections import defaultdict

TRow = dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Operation(ABC):
    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


class Reducer(ABC):
    """Base class for reducers"""

    @abstractmethod
    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        """
        :param rows: table rows
        """
        pass


class Reduce(Operation):  # type ignore
    def __init__(self, reducer: Reducer, keys: tp.Sequence[str]) -> None:  # type ignore
        self.reducer = reducer
        self.keys = keys

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:  # type ignore
        for key_values, group_rows in itertools.groupby(rows, key=lambda row: tuple(row[key] for key in self.keys)):
            yield from self.reducer(tuple(self.keys), group_rows)


class FirstReducer(Reducer):  # type ignore
    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:  # type ignore
        for row in rows:
            yield row
            break


class TopN(Reducer):  # type ignore
    """Calculate top N by value"""

    def __init__(self, column: str, n: int) -> None:  # type ignore
        """
        :param column: column name to get top by
        :param n: number of top values to extract
        """
        self.column = column
        self.n = n

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:  # type ignore
        top_n = heapq.nlargest(self.n, rows, key=lambda row: row.get(self.column, float('-inf')))
        yield from top_n


class TermFrequency(Reducer):
    """Calculate frequency of values in column"""

    def __init__(self, words_column: str, result_column: str = 'tf') -> None:
        """
        :param words_column: name for column with words
        :param result_column: name for result column
        """
        self.words_column = words_column
        self.result_column = result_column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        grouped_data: tp.DefaultDict[tp.Tuple[tp.Any, ...], tp.DefaultDict[tp.Tuple[tp.Any, ...], int]] = (
            defaultdict(lambda: defaultdict(int)))
        group_total: tp.DefaultDict[tp.Tuple[tp.Any, ...], int] = defaultdict(int)

        for row in rows:
            key: tp.Tuple[tp.Any, ...] = tuple(row[k] for k in group_key)
            word = row[self.words_column]
            grouped_data[key][word] += 1
            group_total[key] += 1

        for key, word_counts in grouped_data.items():
            total_count = group_total[key]
            for word, count in word_counts.items():
                new_row = {k: v for k, v in zip(group_key, key)}
                new_row[self.result_column] = count / total_count
                new_row[self.words_column] = word
                yield new_row


class Count(Reducer):
    def __init__(self, column: str) -> None:
        self.column = column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        count = 0
        key_values = None
        for row in rows:
            if key_values is None:
                key_values = tuple(row[key] for key in group_key)
            count += 1
        if key_values is not None:
            new_row = dict(zip(group_key, key_values))
            new_row[self.column] = count
            yield new_row


class Sum(Reducer):
    def __init__(self, column: str) -> None:
        self.column = column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        total = 0
        key_values = None
        for row in rows:
            if key_values is None:
                key_values = tuple(row[key] for key in group_key)
            total += row.get(self.column, 0)
        if key_values is not None:
            new_row = dict(zip(group_key, key_values))
            new_row[self.column] = total
            yield new_row


class Average(Reducer):
    """
        Average values aggregated by key
        Example for key=('a',) and column='b'
            {'a': 1, 'b': 2, 'f': 4}
            {'a': 1, 'b': 8, 'f': 5}
            =>
            {'a': 1, 'b': 5}
        """

    def __init__(self, column: str) -> None:
        """
        :param column: name for average column
        """
        self.column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        grouped_data: tp.DefaultDict[tp.Tuple[tp.Any, ...], int] = defaultdict(int)
        group_count: tp.DefaultDict[tp.Tuple[tp.Any, ...], int] = defaultdict(int)

        for row in rows:
            key: tp.Tuple[tp.Any, ...] = tuple(row[k] for k in group_key)
            grouped_data[key] += row[self.column]
            group_count[key] += 1

        for key, word_counts in grouped_data.items():
            new_row = {k: v for k, v in zip(group_key, key)}
            new_row[self.column] = grouped_data[key] / group_count[key]
            yield new_row
