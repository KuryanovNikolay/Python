from typing import Dict, List
from core.entities.Config import Config
from core.services.Actions import Action
from core.services.TextService.TextFileProcessor import TextFileProcessor


class ReplaceProcessor(TextFileProcessor):
    def replace_chars(self, text: str, file_number: int) -> str:
        replacements: Dict[str, str] = {
            'a': str(1 + file_number),
            'b': str(2 + file_number),
            'c': str(3 + file_number),
        }
        return ''.join(replacements.get(char, char) for char in text)

    def process(self, config: Config) -> Dict[int, Dict[int, str]]:
        file_paths: List[str] = config.path
        action: str = config.action
        number: int = config.id

        if action == Action.REPLACE.value:
            result: Dict[int, Dict[int, str]] = {}
            max_lines: int = 0

            for file_path in file_paths:
                with open(file_path, 'r', encoding='utf-8') as file:
                    lines: List[str] = file.readlines()
                    if len(lines) > max_lines:
                        max_lines = len(lines)

            for k in range(1, number + 1):
                k_lines: Dict[int, str] = {}
                for file_number, file_path in enumerate(file_paths, start=1):
                    with open(file_path, 'r', encoding='utf-8') as file:
                        lines = file.readlines()
                        if k <= len(lines):
                            replaced_line: str = self.replace_chars(
                                lines[k - 1].strip(),
                                file_number
                            )
                            k_lines[file_number] = replaced_line
                        else:
                            k_lines[file_number] = ""
                result[k] = k_lines

            return result
        else:
            return self._pass_to_next(config)