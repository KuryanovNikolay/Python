from abc import ABC, abstractmethod
from typing import override

from core.entities.Config import Config
from core.ports.FileProcessor import FileProcessor


class TextFileProcessor(FileProcessor, ABC):
    def __init__(self):
        self._next_processor = None

    @override
    def set_next_processor(self, processor):
        self._next_processor = processor
        return processor

    def _pass_to_next(self, config):
        if self._next_processor:
            return self._next_processor.process(config)
        return None

    @abstractmethod
    def process(self, config: Config) -> dict:
        pass
