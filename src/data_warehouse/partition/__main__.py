import logging
import data_warehouse.partition.partition as p


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    p.setup_database()
    p.insert_data()
    p.query_data()
        