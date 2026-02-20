import logging
from pathlib import Path

from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
env_path = Path(__file__).resolve().parents[2] / "env"
required_envs = [
    'database_docker.env',
    's3_docker.env',
    'airflow.env',
]

for env_file in env_path.iterdir():
    if env_file.name in required_envs:
        logging.info(f"lading env file {env_file}")
        load_dotenv(env_file)

def main():
    pass

if __name__ == '__main__':
    main()