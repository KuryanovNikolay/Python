import unittest
from unittest.mock import MagicMock
import json
import io
from compgraph import algorithms
from compgraph import operations as ops


class TestGraph(unittest.TestCase):
    def setUp(self) -> None:
        self.sample_data = [
            {"doc_id": "doc1", "text": "hello world hello"},
            {"doc_id": "doc2", "text": "world hello world"}
        ]

        self.expected_pmi_result = [
            {"text": "hello", "doc_id": "doc1", "pmi": 0.8},
            {"text": "world", "doc_id": "doc2", "pmi": 0.7},
        ]

    def test_graph_from_iter(self) -> None:
        graph = algorithms.Graph.graph_from_iter("docs")
        self.assertIsInstance(graph, algorithms.Graph)
        self.assertEqual(len(graph.operations), 1)
        self.assertIsInstance(graph.operations[0], ops.ReadIterFactory)

    def test_graph_from_file(self) -> None:
        parser = MagicMock()
        graph = algorithms.Graph.graph_from_file("dummy.txt", parser)
        self.assertIsInstance(graph, algorithms.Graph)
        self.assertEqual(len(graph.operations), 1)
        self.assertIsInstance(graph.operations[0], ops.Read)

    def test_map_operation(self) -> None:
        mock_mapper = MagicMock()
        graph = algorithms.Graph([]).map(mock_mapper)
        self.assertEqual(len(graph.operations), 1)
        self.assertIsInstance(graph.operations[0], ops.Map)

    def test_reduce_operation(self) -> None:
        mock_reducer = MagicMock()
        graph = algorithms.Graph([]).reduce(mock_reducer, ["key"])
        self.assertEqual(len(graph.operations), 1)
        self.assertIsInstance(graph.operations[0], ops.Reduce)

    def test_join_operation(self) -> None:
        mock_joiner = MagicMock()
        other_graph = algorithms.Graph([])
        graph = algorithms.Graph([]).join(mock_joiner, other_graph, ["key"])
        self.assertEqual(len(graph.operations), 1)
        self.assertIsInstance(graph.operations[0], ops.Join)

    def test_run(self) -> None:
        mock_operation = MagicMock()
        mock_operation.return_value = iter(self.sample_data)

        graph = algorithms.Graph([mock_operation])
        result = list(graph.run(docs=lambda: iter(self.sample_data)))

        self.assertEqual(len(result), len(self.sample_data))
        for res, exp in zip(result, self.sample_data):
            self.assertEqual(res, exp)

    def test_word_count_graph(self) -> None:
        input_data = io.StringIO(json.dumps(self.sample_data))
        graph = algorithms.word_count_graph("docs")
        result = graph.run(docs=lambda: iter(json.loads(input_data.getvalue())))

        result_list = list(result)

        expected_result = [
            {"text": "hello", "count": 3},
            {"text": "world", "count": 3},
        ]

        self.assertEqual(len(result_list), len(expected_result))
        for res, exp in zip(result_list, expected_result):
            self.assertEqual(res["text"], exp["text"])
            self.assertEqual(res["count"], exp["count"])

    def test_inverted_index_graph(self) -> None:
        input_data = io.StringIO(json.dumps(self.sample_data))
        output_data = io.StringIO()
        graph = algorithms.inverted_index_graph("docs")
        result = graph.run(docs=lambda: iter(json.loads(input_data.getvalue())))

        output_data.write(json.dumps(list(result), ensure_ascii=False))
        output_data.seek(0)
        result_data = json.load(output_data)

        self.assertGreater(len(result_data), 0)

    def test_run_pmi(self) -> None:
        input_data = io.StringIO(json.dumps(self.sample_data))
        graph = algorithms.pmi_graph("docs")
        result = graph.run(docs=lambda: iter(json.loads(input_data.getvalue())))

        result_list = list(result)
        print("PMI Graph Result:", result_list)  # Для отладки

        self.assertGreater(len(result_list), 0)
        for item in result_list:
            self.assertIn("text", item)
            self.assertIn("doc_id", item)
            self.assertIn("pmi", item)
            self.assertIsInstance(item["pmi"], float)
