from datetime import datetime, time
import typing as tp
from venv import logger
from telegram import Bot


class TelegramNotification:
    """intervals - интервалы переотправки оповещения, 
    если не получается отправить оповещение"""

    def __init__(
        self,
        chat_id: str,
        token: str,
        message_template: str,
        responsible_users: tp.List[str] = [],
        intervals: tp.List[int] = [1, 60, 600],
    ):
        self._chat_id = chat_id
        self._messageTemplate = MessageTemplate(message_template, responsible_users)
        self._intervals = intervals
        self._token = token

    def send_telegram_notification(self, context: tp.Dict[str, tp.Any]) -> None:

        message = self._messageTemplate.create_message_template(context)

        for interval in self._intervals:
            try:
                bot = Bot(token=self._token)
                bot.send_message(chat_id=self._chat_id, text=message)
                break
            except Exception as e:
                logger.info(f"Error sending message to Telegram: {e}")
                time.sleep(interval)

class MessageTemplate:
    def __init__(self, message_template: str, responsible_users: tp.List[str]):
        self._message_template = message_template
        self._responsible_users = responsible_users

    def create_message_template(self, context: tp.Dict[str, tp.Any]) -> str:
        args = self._parse_context(context)
        message = self._message_template.format(**args)
        return message

    def _parse_context(self, context: tp.Dict[str, tp.Any]) -> tp.Dict[str, tp.Any]:
        """Доступные аргументы для message template
        DAG_NAME: название DAG
        TASK_ID: название задачи
        DATE: дата и время выполнения задачи
        TASK_LOG_URL: ссылка на лог выполнения задачи
        PARAMS: параметры, переданные в задачу
        CONF: глобальные параметры, переданные в DAG при его запуске
        PREV_EXEC_DATE: дата и время выполнения предыдущей задачи
        USERS: список пользоватлей, ответственных за выполнение задачи
        """
        return {
            "DAG_NAME": context.get("dag").dag_id,
            "TASK_ID": context.get("task_instance").task_id,
            "DATE": self._create_formatted_date(context.get("execution_date")),
            "TASK_LOG_URL": context.get("ti").log_url,
            "PARAMS": context.get("params"),
            "CONF": context.get("conf"),
            "PREV_EXEC_DATE": self._create_formatted_date(
                context.get("prev_execution_date")
            ),
            "USERS": self._create_users_string(),
        }

    def _create_formatted_date(self, date: datetime) -> str:
        return date.strftime("%Y-%m-%d %H:%M:%S") if date else ""

    def _create_users_string(self) -> str:
        return ", ".join([f"@{user_name}" for user_name in self._responsible_users])