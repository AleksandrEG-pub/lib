import logging
import os
from dotenv import load_dotenv
import clickhouse_connect as cc
from clickhouse_connect.driver.client import Client
import database.formatter as formatter


class ClickHouseDatabase:
    """ClickHouse database connection manager with lazy configuration"""

    def __init__(self):
        self._client: Client = None

    @property
    def config(self):
        """Lazy-loaded configuration property"""
        if self._client is None:
            load_dotenv(dotenv_path='./clickhouse.env')
            self._client = cc.get_client(
                host=os.getenv('CLICKHOUSE_DB_HOST'),
                port=int(os.getenv('CLICKHOUSE_DB_PORT')),
                database=os.getenv('CLICKHOUSE_DB'),
                username=os.getenv('CLICKHOUSE_USER'),
                password=os.getenv('CLICKHOUSE_PASSWORD'),
            )

    @property
    def client(self):
        if self._client is None:
            self.config
        return self._client

    def _log_query_metrics(self, result, query_name=""):
        summary = result.summary
        logging.info(
            f"{summary['query_id'][:12]:<12} | "
            f"{formatter.format_time_ns(summary['elapsed_ns']):<8} | "
            f"Total:{formatter.format_number(summary['total_rows_to_read']):<6} | "
            f"Read:{formatter.format_number(summary['read_rows']):<6} "
            f"({formatter.format_bytes(summary['read_bytes']):<8}) | "
            f"Result:{formatter.format_number(summary['result_rows'])   } "
            f"({formatter.format_bytes(summary['result_bytes']):<8}) | "
            f"Mem:{formatter.format_bytes(summary['memory_usage']):<8}"
        )

    def execute_command(self, query: str):
        self.client.command(query)

    def execute_query(self, query: str):
        result = self.client.query(query)
        self._log_query_metrics(result)


ch_db = ClickHouseDatabase()
