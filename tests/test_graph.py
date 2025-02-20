# mypy: ignore-errors

import pytest
from compgraph.algorithms import word_count_graph, inverted_index_graph, pmi_graph, yandex_maps_graph


@pytest.fixture
def sample_word_data():
    return [
        {"text": "Hello world! Hello again."},
        {"text": "Testing word count."},
    ]


@pytest.fixture
def sample_document_data():
    return [
        {"doc_id": 1, "text": "Hello world!"},
        {"doc_id": 2, "text": "Hello again world!"},
    ]


@pytest.fixture
def sample_map_data_time():
    return [
        {"edge_id": 1, "enter_time": "20230101T120000.000", "leave_time": "20230101T121000.000"},
        {"edge_id": 2, "enter_time": "20230101T123000.000", "leave_time": "20230101T124500.000"},
    ]


@pytest.fixture
def sample_map_data_length():
    return [
        {"edge_id": 1, "start": [37.0, 55.0], "end": [37.1, 55.1]},
        {"edge_id": 2, "start": [37.2, 55.2], "end": [37.3, 55.3]},
    ]


def test_word_count_graph(sample_word_data):
    graph = word_count_graph(input_stream_name="data", text_column="text", count_column="count")
    result = list(graph.run(data=lambda: iter(sample_word_data)))

    assert len(result) > 0
    assert all("text" in row and "count" in row for row in result)
    assert any(row["text"] == "hello" and row["count"] == 2 for row in result)


def test_inverted_index_graph(sample_document_data):
    graph = inverted_index_graph(input_stream_name="data", doc_column="doc_id", text_column="text")
    result = list(graph.run(data=lambda: iter(sample_document_data)))

    assert len(result) > 0
    assert all("doc_id" in row and "text" in row and "tf_idf" in row for row in result)


def test_pmi_graph():
    sample_document_data = [
        {"doc_id": 1, "text": "hello world hello"},
        {"doc_id": 2, "text": "world again hello"},
    ]

    graph = pmi_graph(input_stream_name="data", doc_column="doc_id", text_column="text")

    def data_stream():
        for item in sample_document_data:
            yield item

    result = list(graph.run(data=data_stream))

    expected_result = [
        {"doc_id": 1, "text": "hello", "pmi": 0.5},
        {"doc_id": 2, "text": "world", "pmi": 0.7},
    ]

    assert len(result) > 0, "Graph returned an empty result"

    for row in result:
        assert "doc_id" in row, "Missing 'doc_id' in result row"
        assert "text" in row, "Missing 'text' in result row"
        assert "pmi" in row, "Missing 'pmi' in result row"
        assert isinstance(row["pmi"], float), "'pmi' should be a float"

    for r, e in zip(result, expected_result):
        assert r["doc_id"] == e["doc_id"]
        assert r["text"] == e["text"]
        assert isinstance(r["pmi"], float)


def test_yandex_maps_graph(sample_map_data_time, sample_map_data_length):
    graph = yandex_maps_graph(
        input_stream_name_time="time_data",
        input_stream_name_length="length_data",
        enter_time_column="enter_time",
        leave_time_column="leave_time",
        edge_id_column="edge_id",
        start_coord_column="start",
        end_coord_column="end",
        weekday_result_column="weekday",
        hour_result_column="hour",
        speed_result_column="speed",
    )

    result = list(graph.run(
        time_data=lambda: iter(sample_map_data_time),
        length_data=lambda: iter(sample_map_data_length),
    ))

    assert len(result) > 0
    assert all("weekday" in row and "hour" in row and "speed" in row for row in result)
