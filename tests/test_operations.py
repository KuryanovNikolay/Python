# mypy: ignore-errors

import pytest
from compgraph import operations as ops
from pytest import approx


@pytest.mark.parametrize('data, expected', [
    (
            [{'test_id': 1, 'value': 10}, {'test_id': 1, 'value': 20}],
            [{'test_id': 1, 'value': 10}]
    ),
    (
            [{'test_id': 2, 'value': 5}, {'test_id': 2, 'value': 15}],
            [{'test_id': 2, 'value': 5}]
    ),
])
def test_first_reducer(data, expected):
    reducer = ops.FirstReducer()
    result = list(reducer(('test_id',), iter(data)))
    assert result == expected


@pytest.mark.parametrize('data, expected', [
    (
            [{'match_id': 1, 'rank': 5}, {'match_id': 1, 'rank': 7}, {'match_id': 1, 'rank': 3}],
            [{'match_id': 1, 'rank': 7}, {'match_id': 1, 'rank': 5}, {'match_id': 1, 'rank': 3}]
    ),
    (
            [{'match_id': 2, 'rank': 2}, {'match_id': 2, 'rank': 10}, {'match_id': 2, 'rank': 1}],
            [{'match_id': 2, 'rank': 10}, {'match_id': 2, 'rank': 2}, {'match_id': 2, 'rank': 1}]
    ),
])
def test_top_n(data, expected):
    reducer = ops.TopN(column='rank', n=3)
    result = list(reducer(('match_id',), iter(data)))
    assert result == expected


@pytest.mark.parametrize('data, expected', [
    (
            [{'doc_id': 1, 'text': 'hello', 'count': 1}, {'doc_id': 1, 'text': 'hello', 'count': 2}],
            [{'doc_id': 1, 'text': 'hello', 'tf': approx(1.0)}]
    ),
    (
            [{'doc_id': 1, 'text': 'hello', 'count': 1}, {'doc_id': 1, 'text': 'world', 'count': 1}],
            [{'doc_id': 1, 'text': 'hello', 'tf': approx(0.5)}, {'doc_id': 1, 'text': 'world', 'tf': approx(0.5)}]
    ),
])
def test_term_frequency(data, expected):
    reducer = ops.TermFrequency(words_column='text')
    result = list(reducer(('doc_id',), iter(data)))
    for r, e in zip(result, expected):
        assert r['tf'] == approx(e['tf'], rel=1e-3)
    assert result == expected


@pytest.mark.parametrize('data, expected', [
    (
            [{'text': 'Hello, world!'}],
            [{'text': 'Hello world'}]
    ),
    (
            [{'text': 'Test. punctuation.'}],
            [{'text': 'Test punctuation'}]
    ),
])
def test_filter_punctuation(data, expected):
    mapper = ops.FilterPunctuation(column='text')
    result = [list(mapper(row))[0] for row in data]
    assert result == expected


@pytest.mark.parametrize('data, expected', [
    (
            [{'text': 'CamelCase'}],
            [{'text': 'camelcase'}]
    ),
    (
            [{'text': 'UPPERCASE'}],
            [{'text': 'uppercase'}]
    ),
])
def test_lower_case(data, expected):
    mapper = ops.LowerCase(column='text')
    result = [list(mapper(row))[0] for row in data]
    assert result == expected


@pytest.mark.parametrize('data, expected', [
    (
            [{'start_coords': (10.0, 20.0), 'end_coords': (20.0, 30.0)}],
            [{'start_coords': (10.0, 20.0), 'end_coords': (20.0, 30.0), 'distance': 1499.57}]
    ),
])
def test_calculate_distance(data, expected):
    mapper = ops.Calculate(
        operation=ops.haversine_distance,
        params={'start_coords': 'start_coords', 'end_coords': 'end_coords'},
        result_column='distance'
    )
    result = [list(mapper(row))[0] for row in data]
    for r, e in zip(result, expected):
        assert r['distance'] == approx(e['distance'], rel=1e-2)


@pytest.mark.parametrize('data, expected', [
    (
            [{'enter_time': '20220101T120000.000', 'leave_time': '20220101T121000.000'}],
            [{'enter_time': '20220101T120000.000', 'leave_time': '20220101T121000.000', 'time_in_sec': 600}]
    ),
])
def test_calculate_time(data, expected):
    mapper = ops.Calculate(operation=ops.road_time, params={'enter_time': 'enter_time', 'leave_time': 'leave_time'},
                           result_column='time_in_sec')
    result = [list(mapper(row))[0] for row in data]
    for r, e in zip(result, expected):
        assert r['time_in_sec'] == e['time_in_sec']


@pytest.mark.parametrize('data, expected', [
    (
            [{'datetime_column': '20220101T120000.000'}],
            [{'datetime_column': '20220101T120000.000', 'weekday': 'Sat'}]
    ),
])
def test_calculate_weekday(data, expected):
    mapper = ops.Calculate(operation=ops.weekday, params={'datetime_column': 'datetime_column'},
                           result_column='weekday')
    result = [list(mapper(row))[0] for row in data]
    for r, e in zip(result, expected):
        assert r['weekday'] == e['weekday']


@pytest.mark.parametrize('data, expected', [
    (
            [{'datetime_column': '20220101T123000.000'}],
            [{'datetime_column': '20220101T123000.000', 'hour': 12}]
    ),
])
def test_calculate_hour(data, expected):
    mapper = ops.Calculate(operation=ops.hour, params={'datetime_column': 'datetime_column'}, result_column='hour')
    result = [list(mapper(row))[0] for row in data]
    for r, e in zip(result, expected):
        assert r['hour'] == e['hour']
