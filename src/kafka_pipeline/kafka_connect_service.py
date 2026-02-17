import logging
import os
from pathlib import Path
import requests
import json

CONNECTOR_NAME = "pg-connector"

def _connector_exists(name, connect_url):
    resp = requests.get(f"{connect_url}/{name}")
    return resp.status_code == 200

def create_connector(config, connect_url):
    response = requests.post(connect_url, headers={"Content-Type": "application/json"},
                         data=json.dumps(config))
    if response.status_code in (200, 201):
        logging.info(f"Connector '{config['name']}' created successfully.")
    else:
        logging.info(f"Failed to create connector: {response.status_code} {response.text}")

def init_connector():
    logging.info("setup kafka_connect")
    config_file = Path(__file__).resolve().parents[2] / "env" / "kafka_connect_postgres.json"
    with open(config_file, "r") as f:
        connector_config = json.load(f)
        connect_url = f"http://{os.getenv('KAFKA_CONNECT_HOST_PORT')}/connectors"
        if _connector_exists(CONNECTOR_NAME, connect_url):
            logging.info(f"Connector '{CONNECTOR_NAME}' already exists.")
        else:
            logging.info(f"Connector '{CONNECTOR_NAME}' not exists, creating")
            create_connector(connector_config, connect_url)
