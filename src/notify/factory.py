from typing import Type

from src.notify.base import BaseNotifier
from src.notify.email import EmailNotifier
from src.notify.webhook import WebhookNotifier


class NotifierFactory:
    _notifiers = {
        "email": EmailNotifier,
        "webhook": WebhookNotifier,
    }

    @classmethod
    def get_notifier(self, notifier_type: str) -> Type[BaseNotifier]:
        return self._notifiers[notifier_type]
