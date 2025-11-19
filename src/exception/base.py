from abc import ABC, abstractmethod
from typing import Any


class BaseFileErrorEmailException(Exception, ABC):
    def __init__(self, error_values: dict[str, Any]):
        super().__init__()
        self.error_values: dict[str, Any] = error_values

    @property
    @abstractmethod
    def email_message(self) -> str:
        pass
