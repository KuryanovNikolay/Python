from infrastructure.ConfigRepository import ConfigRepository


class InMemoryConfigRepository(ConfigRepository):
    def __init__(self) -> None:
        self.configurations: dict = dict()

    def add(self, config_id: int, config_data: bool) -> None:
        self.configurations[config_id] = config_data

    def get(self, config_id: int or None) -> bool or None:
        return self.configurations.get(config_id)

    def remove(self, config_id: int) -> None:
        if config_id in self.configurations:
            del self.configurations[config_id]
