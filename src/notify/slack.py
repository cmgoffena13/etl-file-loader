import logging
from enum import Enum
from typing import Any, Dict, Optional

import pendulum
from slack_sdk.webhook import WebhookClient

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


class SlackNotifier(BaseNotifier):
    def __init__(
        self,
        level: AlertLevel,
        title: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.level = level
        self.title = title
        self.message = message
        self.details = details
        self.slack_message = self._create_message()

    def _create_message(self) -> str:
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

        return "\n".join(formatted_message)

    @retry()
    def _send_slack(self):
        webhook = WebhookClient(config.SLACK_WEBHOOK_URL)
        response = webhook.send(text=self.slack_message)
        if response.status_code == 200:
            logger.info("Sent Slack notification for internal processing error")
        else:
            raise Exception(
                f"Slack webhook returned status {response.status_code}: {response.body}"
            )

    def notify(self):
        if not config.SLACK_WEBHOOK_URL:
            logger.warning(
                "SLACK_WEBHOOK_URL not configured, skipping Slack notification"
            )
            return

        try:
            self._send_slack()
        except Exception as e:
            logger.exception(f"Failed to send Slack notification after retries: {e}")
