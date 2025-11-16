import logging
import smtplib
import textwrap
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from src.notify.base import BaseNotifier
from src.settings import config
from src.utils import retry

logger = logging.getLogger(__name__)


class EmailNotifier(BaseNotifier):
    def __init__(
        self,
        source_filename: str,
        exception: Exception,
        recipient_emails: list[str],
        log_id: str = None,
        additional_details: str = None,
        **kwargs: Any,
    ):
        self.source_filename = source_filename
        self.exception = exception
        self.recipient_emails = recipient_emails
        self.log_id = log_id
        self.email_message = self._create_email_message()
        self.additional_details = additional_details

    def _format_email_message(self, **kwargs: Any) -> str:
        return self.exception.email_message.format(**kwargs)

    def _create_email_message(self) -> str:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = (
            f"FileLoader Failed: {self.source_filename} - {self.exception.error_type}"
        )
        if config.FROM_EMAIL is None:
            logger.warning("FROM_EMAIL not configured, skipping email notification")
            return
        msg["From"] = config.FROM_EMAIL

        msg["To"] = ", ".join(self.recipient_emails)
        if config.DATA_TEAM_EMAIL:
            msg["Cc"] = config.DATA_TEAM_EMAIL

        body_text = textwrap.dedent(f"""
        File Processing Failure Notification

        File: {self.source_filename}
        Error Type: {self.exception.error_type}
        Log ID: {self.log_id if self.log_id else "N/A"}

        Error Details:
        {self._format_email_message()}
        """).strip()

        if self.additional_details:
            body_text += f"\nAdditional Information:\n{self.additional_details}"

        if self.log_id:
            body_text += (
                f"\n\nData Team can reference log_id={self.log_id} for more details."
            )

        msg.attach(MIMEText(body_text, "plain"))
        return msg

    @retry()
    def _send_email(self):
        if not config.SMTP_HOST:
            logger.warning("SMTP_HOST not configured, skipping email notification")
            return

        # Use SMTP_SSL for port 465, regular SMTP for other ports
        if config.SMTP_PORT == 465:
            server = smtplib.SMTP_SSL(config.SMTP_HOST, config.SMTP_PORT)
        else:
            server = smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT)

        with server:
            if config.SMTP_USER and config.SMTP_PASSWORD:
                # Only start TLS if not using SSL (port 465)
                if config.SMTP_PORT != 465:
                    server.starttls()
                server.login(config.SMTP_USER, config.SMTP_PASSWORD)

            all_recipients = self.recipient_emails + config.DATA_TEAM_EMAIL
            server.sendmail(
                config.FROM_EMAIL, all_recipients, self.email_message.as_string()
            )
            logger.info(
                f"Sent failure notification email for {self.source_filename} to {len(all_recipients)} recipient(s)"
            )

    @classmethod
    def notify(self, emails: list[str], message: str):
        try:
            self._send_email()
        except Exception as e:
            logger.exception(
                f"Failed to send notification email for {self.source_filename} after retries: {e}"
            )
