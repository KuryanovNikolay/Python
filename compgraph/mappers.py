import datetime
import re
import string
import typing as tp
from abc import abstractmethod, ABC
from copy import deepcopy, copy
from math import log, radians, asin, sin, pow, sqrt, cos

TRow = dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Operation(ABC):
    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


class Mapper(ABC):
    """Base class for mappers"""

    @abstractmethod
    def __call__(self, row: TRow) -> TRowsGenerator:
        """
        :param row: one table row
        """
        pass


class Map(Operation):
    def __init__(self, mapper: Mapper) -> None:
        self.mapper = mapper

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in rows:
            for mapped_row in self.mapper(row):
                yield mapped_row


class DummyMapper(Mapper):
    """Yield exactly the row passed"""

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield row


class FilterPunctuation(Mapper):  # type ignore
    """Left only non-punctuation symbols"""

    def __init__(self, column: str):  # type ignore
        """
        :param column: name of column to process
        """
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:  # type ignore
        row_copy = row.copy()
        if self.column in row_copy:
            row_copy[self.column] = ''.join(
                char for char in str(row_copy[self.column]) if char not in string.punctuation
            )
        yield row_copy


class LowerCase(Mapper):  # type ignore
    """Replace column value with value in lower case"""

    def __init__(self, column: str):  # type ignore
        """
        :param column: name of column to process
        """
        self.column = column

    @staticmethod
    def _lower_case(txt: str) -> str:
        return txt.lower()

    def __call__(self, row: TRow) -> TRowsGenerator:
        row_copy = row.copy()
        if self.column in row_copy:
            row_copy[self.column] = str(row_copy[self.column]).lower()
        yield row_copy


class Calculate(Mapper):
    def __init__(self, operation: tp.Callable, params: tp.Dict[str, str], result_column: str) -> None:  # type: ignore
        self.operation = operation
        self.params = params
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        result = self.operation(row, **self.params)
        copied_row = deepcopy(row)
        copied_row[self.result_column] = result
        yield copied_row


def haversine_distance(row: TRow, start_coords: str, end_coords: str) -> float:
    R = 6373.0
    start_lon, start_lat = [radians(coord) for coord in row[start_coords]]
    end_lon, end_lat = [radians(coord) for coord in row[end_coords]]

    archaversine = asin(sqrt(
        pow(sin((end_lat - start_lat) / 2), 2) + cos(start_lat) * cos(end_lat) * pow(sin((end_lon - start_lon) / 2),
                                                                                     2)))
    return 2 * R * archaversine


def road_time(row: TRow, enter_time: str, leave_time: str) -> float:
    formatted_enter_time = datetime.datetime.strptime(row[enter_time], "%Y%m%dT%H%M%S.%f")
    formatted_leave_time = datetime.datetime.strptime(row[leave_time], "%Y%m%dT%H%M%S.%f")
    return (formatted_leave_time - formatted_enter_time).total_seconds()


def weekday(row: TRow, datetime_column: str) -> str:
    all_weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    return all_weekdays[datetime.datetime.strptime(row[datetime_column], "%Y%m%dT%H%M%S.%f").weekday()]


def hour(row: TRow, datetime_column: str) -> int:
    return datetime.datetime.strptime(row[datetime_column], "%Y%m%dT%H%M%S.%f").hour


def speed(row: TRow, distance: str, time: str) -> float:
    from_meters_per_second = 3600  # Перевод из метров в секунду в километры в час
    return row[distance] / row[time] * from_meters_per_second


class Split(Mapper):
    """Split row on multiple rows by separator"""

    def __init__(self, column: str, separator: str = r'\s+') -> None:
        """
        :param column: name of column to split
        :param separator: string to separate by
        """
        self.column = column
        self.separator = separator

    def __call__(self, row: TRow) -> TRowsGenerator:
        last_split_index: int = 0
        for separator_match in re.finditer(re.compile(self.separator), row[self.column]):
            new_row = copy(row)
            new_row.update({self.column: row[self.column][last_split_index:separator_match.start()]})
            last_split_index = separator_match.end()
            yield new_row
        if len(row[self.column]) != last_split_index:
            new_row = copy(row)
            new_row.update({self.column: row[self.column][last_split_index:len(row[self.column])]})
            yield new_row


class ReverseFreq(Mapper):
    """Calculates inversion of the frequency with which a certain word occurs in the collection documents"""

    def __init__(self, total_docs_column: str, docs_column: str, result_column: str = 'idf') -> None:
        """
        :param total_docs_column: column with total docs
        :param docs_column: name where word_i is present
        :param result_column: name for result column
        """
        self.docs_column = docs_column
        self.total_docs_column = total_docs_column
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        copied_row: TRow = deepcopy(row)
        copied_row[self.result_column] = log(row[self.total_docs_column]) - log(row[self.docs_column])
        yield copied_row


class Product(Mapper):  # type ignore
    """Calculates product of multiple columns"""

    def __init__(self, columns: tp.Sequence[str], result_column: str = 'product') -> None:  # type ignore
        """
        :param columns: column names to product
        :param result_column: column name to save product in
        """
        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:  # type ignore
        row_copy = row.copy()
        product = 1
        for column in self.columns:
            product *= row_copy.get(column, 1)
        row_copy[self.result_column] = product
        yield row_copy


class Filter(Mapper):  # type ignore
    """Remove records that don't satisfy some condition"""

    def __init__(self, condition: tp.Callable[[TRow], bool]) -> None:  # type ignore
        """
        :param condition: if condition is not true - remove record
        """
        self.condition = condition

    def __call__(self, row: TRow) -> TRowsGenerator:  # type ignore
        if self.condition(row):
            yield row


class Project(Mapper):  # type ignore
    """Leave only mentioned columns"""

    def __init__(self, columns: tp.Sequence[str]) -> None:  # type ignore
        """
        :param columns: names of columns
        """
        self.columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:  # type ignore
        yield {col: row[col] for col in self.columns if col in row}
