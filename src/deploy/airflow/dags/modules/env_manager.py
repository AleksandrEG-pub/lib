import logging
from pathlib import Path
from dotenv import load_dotenv


def init_env(env_list: list[str]):
    env_path = Path(__file__).resolve().parent / "env"
    for env_file in env_path.iterdir():
        if env_file.name in env_list:
            logging.info(f"lading env file {env_file}")
            load_dotenv(env_file)
