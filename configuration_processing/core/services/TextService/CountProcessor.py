from typing import Dict, List
from core.entities.Config import Config
from core.services.Actions import Action
from core.services.TextService.TextFileProcessor import TextFileProcessor


class CountProcessor(TextFileProcessor):
    def count_words(self, text: str) -> int:
        return len(text.split())

    def initialize_result_map(self, number: int, count_files: int) -> Dict[int, Dict[int, int]]:
        result: Dict[int, Dict[int, int]] = {}
        for k in range(1, number + 1):
            result[k] = {}
            for file_number in range(1, count_files + 1):
                result[k][file_number] = 0
        return result

    def process(self, config: Config) -> Dict[int, Dict[int, int]]:
        file_paths: List[str] = config.path
        action: str = config.action
        number: int = config.id

        if action == Action.COUNT.value:
            result: Dict[int, Dict[int, int]] = self.initialize_result_map(number, len(file_paths))

            for file_number, file_path in enumerate(file_paths, start=1):
                with open(file_path, 'r', encoding='utf-8') as file:
                    lines: List[str] = file.readlines()
                    for k in range(1, number + 1):
                        if k <= len(lines):
                            result[k][file_number] = self.count_words(lines[k - 1].strip())
                        else:
                            result[k][file_number] = 0

            return result
        else:
            return self._pass_to_next(config)