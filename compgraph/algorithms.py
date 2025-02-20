from . import Graph
from . import operations
import copy


def word_count_graph(input_stream_name: str, text_column: str = 'text', count_column: str = 'count') -> Graph:
    """Constructs graph which counts words in text_column of all rows passed"""
    return Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([text_column]) \
        .reduce(operations.Count(count_column), [text_column]) \
        .sort([count_column, text_column])


def inverted_index_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
                         result_column: str = 'tf_idf') -> Graph:
    """Constructs graph which calculates tf-idf for every word/document pair"""

    graph = Graph.graph_from_iter(input_stream_name)

    split_word = copy.deepcopy(graph) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.Split(text_column))

    total_docs_column = 'total_number_docs'
    count_docs = graph.reduce(operations.Count(total_docs_column), [])

    docs_word_present = 'docs_word_present'

    def idf_operation(row: operations.TRow, total_docs_column: str, docs_column: str) -> float:
        """Функция для вычисления IDF"""
        return operations.log(row[total_docs_column]) - operations.log(row[docs_column])

    count_idf = copy.deepcopy(split_word) \
        .sort([doc_column, text_column]) \
        .reduce(operations.FirstReducer(), [doc_column, text_column]) \
        .sort([text_column]) \
        .reduce(operations.Count(docs_word_present), [text_column]) \
        .join(operations.InnerJoiner(), count_docs, []) \
        .map(
        operations.Calculate(idf_operation, {'total_docs_column': total_docs_column, 'docs_column': docs_word_present},
                             'idf')) \
        .sort([text_column])

    tf = split_word.sort([doc_column]) \
        .reduce(operations.TermFrequency(text_column), [doc_column]) \
        .sort([text_column])

    return tf.join(operations.InnerJoiner(), count_idf, [text_column]) \
        .map(operations.Product(['tf', 'idf'], result_column)) \
        .sort([text_column]) \
        .map(operations.Project([text_column, doc_column, result_column])) \
        .reduce(operations.TopN(result_column, 3), [text_column])


def pmi_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
              result_column: str = 'pmi') -> Graph:
    """Constructs graph which gives for every document the top 10 words ranked by pointwise mutual information"""
    graph = Graph.graph_from_iter(input_stream_name)

    split_word = copy.deepcopy(graph) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([doc_column, text_column])

    result_column_count, result_column_tf, tf_total_docs_column = 'count_column', 'tf_all_column', 'tf'

    count_doc_words = split_word.reduce(operations.Count(result_column_count), [doc_column, text_column])

    words_filtered = split_word \
        .join(operations.OuterJoiner(), count_doc_words, [doc_column, text_column]) \
        .map(operations.Filter(lambda x: (x[result_column_count] >= 2) and (len(x[text_column]) >= 4)))

    tf = words_filtered.sort([doc_column]) \
        .reduce(operations.TermFrequency(text_column), [doc_column]) \
        .sort([text_column])

    all_tf = words_filtered \
        .reduce(operations.TermFrequency(text_column, result_column_tf), []) \
        .map(operations.Project([result_column_tf, text_column])) \
        .sort([text_column])

    return tf \
        .join(operations.OuterJoiner(), all_tf, [text_column]) \
        .map(operations.ReverseFreq(tf_total_docs_column, result_column_tf, result_column)) \
        .sort([doc_column]) \
        .map(operations.Project([text_column, doc_column, result_column])) \
        .reduce(operations.TopN(result_column, 10), [doc_column])


def yandex_maps_graph(input_stream_name_time: str, input_stream_name_length: str,
                      enter_time_column: str = 'enter_time', leave_time_column: str = 'leave_time',
                      edge_id_column: str = 'edge_id', start_coord_column: str = 'start', end_coord_column: str = 'end',
                      weekday_result_column: str = 'weekday', hour_result_column: str = 'hour',
                      speed_result_column: str = 'speed') -> Graph:
    """Constructs graph which measures average speed in km/h depending on the weekday and hour"""

    graph_time = Graph.graph_from_iter(input_stream_name_time)

    graph_length = Graph.graph_from_iter(input_stream_name_length) \
        .map(operations.Calculate(operations.haversine_distance,
                                  {'start_coords': start_coord_column, 'end_coords': end_coord_column}, 'distance')) \
        .sort([edge_id_column])

    graph_time_transformed = graph_time \
        .sort([edge_id_column]) \
        .map(operations.Calculate(operations.road_time,
                                  {'enter_time': enter_time_column, 'leave_time': leave_time_column}, 'road_time')) \
        .map(operations.Calculate(operations.hour,
                                  {'datetime_column': enter_time_column}, hour_result_column)) \
        .map(operations.Calculate(operations.weekday,
                                  {'datetime_column': enter_time_column}, weekday_result_column))

    joined_graph = graph_time_transformed.join(operations.InnerJoiner(), graph_length, [edge_id_column])

    graph_with_speed = joined_graph \
        .map(operations.Calculate(operations.speed,
                                  {'distance': 'distance', 'time': 'road_time'}, speed_result_column)) \
        .sort([weekday_result_column, hour_result_column])

    result_graph = graph_with_speed.reduce(operations.Average(speed_result_column),
                                           [weekday_result_column, hour_result_column])

    return result_graph.map(operations.Project([weekday_result_column, hour_result_column, speed_result_column]))
