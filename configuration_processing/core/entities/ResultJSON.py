from dataclasses import dataclass
from typing import Dict, List


@dataclass
class ProcessingResult:
    config_file: str
    configuration_id: int
    configuration_data: Dict[str, str]
    out: Dict[int, Dict[int, str]]
