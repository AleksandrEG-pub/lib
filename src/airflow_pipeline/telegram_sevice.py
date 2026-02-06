import os
import time
from datetime import datetime
import logging
import asyncio
from telegram import Bot
from telegram.error import TelegramError

logger = logging.getLogger(__name__)


class TelegramChatNotification:
    """Send simple notifications to Telegram.
    """

    def __init__(self, chat_id: str, token: str, retries: int = 3, retry_delay: int = 5):
        self._chat_id = chat_id
        self._bot = Bot(token=token)
        self._retries = retries
        self._retry_delay = retry_delay

    def send(self, dag_id: str, dag_execution_time: datetime, result: str, details: str) -> None:
        """Send a message to chat"""
        message = f"""
        DAG: {dag_id}
        execution: {dag_execution_time}
        result: {result}
        details: {details}
        """
        for attempt in range(1, self._retries + 1):
            try:
                asyncio.run(self._bot.send_message(chat_id=self._chat_id, text=message))
                break
            except TelegramError as e:
                logger.warning(f"Attempt {attempt} failed: {e}")
                if attempt < self._retries:
                    time.sleep(self._retry_delay)
                else:
                    logger.error("Failed to send Telegram notification after retries.")

notification = TelegramChatNotification(
                    chat_id=os.getenv('TG_CHAT_ID'),
                    token=os.getenv('TG_TOKEN'),
                    retries=3,
                    retry_delay=60,
                )
