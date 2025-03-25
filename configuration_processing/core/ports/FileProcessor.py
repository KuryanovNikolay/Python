from abc import abstractstaticmethod, ABC, abstractmethod

from core.entities.Config import Config


class FileProcessor(ABC):
    @abstractmethod
    def process(self, config: Config):
        pass

    @abstractmethod
    def set_next_processor(self, processor):
        pass