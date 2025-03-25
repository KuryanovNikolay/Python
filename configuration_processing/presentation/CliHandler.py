from core.services.TextService.TextService import TextService
from infrastructure.InMemoryConfigRepository import InMemoryConfigRepository


class CliHandler:
    def __init__(self, config_file, config_number):
        self.config_file = config_file
        self.config_number = config_number
        self.in_memory_repository = InMemoryConfigRepository()

    def handle(self):
        try:
            text_service = TextService(self.in_memory_repository)
            output_path = text_service.process(self.config_file, self.config_number)
            print(output_path)
            return output_path
        except Exception as e:
            print(f"Ошибка: {e}")
