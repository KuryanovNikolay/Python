from dataclasses import dataclass


@dataclass
class Config:
    id: int
    mode: str
    path: list[str]
    action: str