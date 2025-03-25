from abc import ABC, abstractmethod


class ConfigRepository(ABC):
    @abstractmethod
    def add(self, config_id: int, config_data: bool) -> None:
        pass

    @abstractmethod
    def get(self, config_id: int) -> bool:
        pass

    @abstractmethod
    def remove(self, config_id: int) -> None:
        pass
