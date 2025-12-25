import logging
import data_warehouse.data_vault.data_vault as dv


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    dv.setup_database()
    