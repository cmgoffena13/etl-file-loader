from abc import ABC, abstractmethod


class BaseNotifier(ABC):
    @abstractmethod
    def _create_message(self) -> str:
        pass

    @abstractmethod
    def notify(self):
        pass
