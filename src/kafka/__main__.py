import logging
import kafka.data_service as data_service
import kafka.sql_service as sql_service

def main():
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
    sql_service.init_sql()
    data_service.init_data()
    data_service.upload_from_database_to_kafka()
    data_service.sink_from_kafka_to_database()

if __name__ == '__main__':
    main()
    