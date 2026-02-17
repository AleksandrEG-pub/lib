import logging
import sys
from pathlib import Path

from dotenv import load_dotenv
from dbt_star import sql_service

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
env_path = Path(__file__).resolve().parents[2] / "env"
required_envs = ['database_docker.env',]

for env_file in env_path.iterdir():
    if env_file.name in required_envs:
        logging.info(f"lading env file {env_file}")
        load_dotenv(env_file)
        
def main():
    mode = sys.argv[1]
    logging.info(f"mode: {mode}")
    if mode == 'init':
        sql_service.init_tables()  
        if sql_service.tables_empty():
            sql_service.init_data()  
    if mode == 'update':
        sql_service.update_data()  

if __name__ == '__main__':
    main()