import logging
from pathlib import Path
from dotenv import load_dotenv
from ci_cd import sql_service

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")

env_path = Path(__file__).resolve().parents[2] / "env"
required_envs = ['database_docker.env',]

for env_file in env_path.iterdir():
    if env_file.name in required_envs:
        logging.info(f"loading env file {env_file}")
        load_dotenv(env_file)

def main():
    sql_service.init_tables()
    sql_service.init_data()
    sql_service.migrate_data()

if __name__ == "__main__":
    main()
    
