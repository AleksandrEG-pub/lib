import logging
import s3_upload.etl_s3 as etls3

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("loading to s3 from postgres")
    etls3.init_database()
    data = etls3.load_products()
    etls3.s3_upload_parquet(data)
    etls3.s3_upload_iceberg(data)
    
    # 1. Развернуть сервер S3 (например, MinIO, см. статью о сервере MinIO), 
    #   либо использовать подключение к сервису одного из провайдеров (например, Yandex Cloud)
    # 2. Написать Python-скрипт чтения данных из PostgreSQL, 
    #   их преобразования и сохранения полученного результата в S3-совместимое хранилище в формате Parquet и в формате Iceberg 
    #   с использованием одной из библиотек
    # 3. Сравнить содержание целевых данных в S3 для разных форматов
    # Ожидаемый результат:
    # 1. Данные, записанные в таблицу PosrgreSQL после запуска скрипта транслируются в S3
    # 2. Описание содержимого данных в S3 в формате Parquet и Iceberg – что присутствует в каждом, 
    # назначение полученных объектов, отличия

if __name__ == "__main__":
    main()
    
