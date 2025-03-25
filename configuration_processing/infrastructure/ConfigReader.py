import os
from core.entities import Config


class ConfigReader:
    _config: Config = Config

    def get_config(self, path, number) -> Config:
        found_id: bool = False
        with open(path, 'r') as file:
            for line in file:
                line: str = line.strip()
                if line.startswith("#id:"):
                    current_id: int = int(line.split(":")[1].strip())
                    if current_id == number:
                        self._config.id = current_id
                        found_id = True
                elif found_id:
                    if line.startswith("#mode:"):
                        self._config.mode = line.split(":")[1].strip()
                    elif line.startswith("#path:"):
                        paths: str = line.split(": ")[1]
                        raw_paths: list[str] = [p.strip() for p in paths.split(",")]

                        if self._config.mode == "dir":
                            self._config.path = []
                            for dir_path in raw_paths:
                                if os.path.isdir(dir_path):
                                    files: list[str] = [os.path.join(dir_path, f) for f in os.listdir(dir_path)
                                                        if os.path.isfile(os.path.join(dir_path, f))]
                                    self._config.path.extend(files)
                                else:
                                    raise Exception(f"Directory not found: {dir_path}")
                        else:
                            self._config.path = raw_paths

                    elif line.startswith("#action:"):
                        self._config.action = line.split(":")[1].strip()
                        break
        if not found_id:
            raise Exception(f"Config id not found: {path}")
        return self._config
