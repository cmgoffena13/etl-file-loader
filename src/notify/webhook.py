import logging
from enum import Enum
from typing import Any, Dict, Optional

import httpx
import pendulum

from src.notify.base import BaseNotifier
from src.settings import config
from src.utils import retry

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    INFO = "ℹ️"
    WARNING = "⚠️"
    ERROR = "❌"
    CRITICAL = "🚨"
    SUCCESS = "✅"


class WebhookNotifier(BaseNotifier):
    def __init__(
        self,
        level: AlertLevel,
        title: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        webhook_url: Optional[str] = None,
    ):
        self.level = level
        self.title = title
        self.message = message
        self.details = details
        self.webhook_url = webhook_url or config.WEBHOOK_URL
        self.webhook_message = self._create_message()

    def _create_message(self) -> Dict[str, Any]:
        timestamp = pendulum.now("UTC").format("YYYY-MM-DD HH:mm:ss z")

        formatted_message = [
            f"{self.level.value} *{self.level.name}*",
            f"*{self.title}*",
            f"*Timestamp:* {timestamp}",
            f"*Message:* {self.message}",
        ]

        if self.details:
            detail_lines = []
            for key, value in self.details.items():
                detail_lines.append(f"• *{key}:* {value}")
            if detail_lines:
                formatted_message.append("\n*Details:*")
                formatted_message.extend(detail_lines)

        text_message = "\n".join(formatted_message)

        payload = {"text": text_message}
        payload["title"] = self.title
        payload["timestamp"] = timestamp
        payload["level"] = self.level.name
        if self.details:
            payload["details"] = self.details

        return payload

    @retry()
    def _send_webhook(self):
        if not self.webhook_url:
            raise ValueError("Webhook URL not configured")

        response = httpx.post(
            self.webhook_url,
            json=self.webhook_message,
            timeout=10.0,
        )
        if response.status_code == 200:
            logger.info("Sent webhook notification successfully")
        else:
            raise Exception(
                f"Webhook returned status {response.status_code}: {response.text}"
            )

    def notify(self):
        if not self.webhook_url:
            logger.warning("WEBHOOK_URL not configured, skipping webhook notification")
            return

        try:
            self._send_webhook()
        except Exception as e:
            logger.exception(f"Failed to send webhook notification after retries: {e}")
