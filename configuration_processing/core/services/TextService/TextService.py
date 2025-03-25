from core.ports.FileProcessor import FileProcessor
from core.services.TextService.CountProcessor import CountProcessor
from core.services.TextService.ReplaceProcessor import ReplaceProcessor
from core.services.TextService.StringProcessor import StringProcessor
from infrastructure.ConfigReader import ConfigReader
from infrastructure.InMemoryConfigRepository import InMemoryConfigRepository
from infrastructure.ResultJSONDAO import ResultDAO


class TextService:
    def __init__(self, in_memory_repository: InMemoryConfigRepository):
        self.in_memory_repository = in_memory_repository

    def process(self, path, number):
        config_reader: ConfigReader = ConfigReader()
        config = config_reader.get_config(path, number)

        if not self.in_memory_repository.get(config.id):
            string_processor: FileProcessor = StringProcessor()
            count_processor: FileProcessor = CountProcessor()
            replace_processor: FileProcessor = ReplaceProcessor()

            (string_processor
             .set_next_processor(replace_processor)
             .set_next_processor(count_processor))

            result_DAO: ResultDAO = ResultDAO()
            saved_file: str = result_DAO.save_to_json(string_processor.process(config), path, config)
            self.in_memory_repository.add(config.id, True)

            return saved_file
