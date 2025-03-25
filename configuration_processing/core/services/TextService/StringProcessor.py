from typing import Dict, List
from core.services.TextService.TextFileProcessor import TextFileProcessor
from core.entities.Config import Config
from core.services.Actions import Action


class StringProcessor(TextFileProcessor):
    def process(self, config: Config) -> Dict[int, Dict[int, str]]:
        file_paths: List[str] = config.path
        action: str = config.action
        number: int = config.id

        if action != Action.STRING.value:
            return self._pass_to_next(config)

        result: Dict[int, Dict[int, str]] = {}
        max_lines: int = 0

        for file_path in file_paths:
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    lines: List[str] = file.readlines()
                    max_lines = max(max_lines, len(lines))
            except IOError as e:
                raise IOError(f"Ошибка чтения файла {file_path}: {str(e)}")

        for k in range(1, number + 1):
            k_lines: Dict[int, str] = {}

            for file_number, file_path in enumerate(file_paths, start=1):
                try:
                    with open(file_path, 'r', encoding='utf-8') as file:
                        lines = file.readlines()
                        k_lines[file_number] = lines[k - 1].strip() if k <= len(lines) else ""
                except IOError as e:
                    raise IOError(f"Ошибка чтения файла {file_path}: {str(e)}")

            result[k] = k_lines

        return result