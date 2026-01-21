import os
import tomllib
from typing import Any, Dict
from venv import logger


script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)
queries_file = f"{script_dir}/sql/clickhouse_queries.toml"


class Queries:
    '''
    Loads queries from file in form of dictiionary
    '''
    def __init__(self):
        self._config: Dict = {}

    def config(self) -> Dict[str, Any]:
        if self._config == None or len(self._config) == 0:
            self._config = self._load_config()
        return self._config

    def _load_config(self) -> Dict[str, Any]:
        try:
            with open(queries_file, 'rb') as f:
                return tomllib.load(f)
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {queries_file}")
            raise
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            raise


queries_config = Queries()
