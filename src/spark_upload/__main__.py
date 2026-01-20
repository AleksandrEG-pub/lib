import logging
import spark_upload.sql_service as sql
import spark_upload.product_service as ps
from s3_upload.s3_manager import s3manager


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logging.info("creating table 'products' in postgres")
    sql.init_database()

    logging.info("init s3 bucket")
    s3manager.init_bucket(s3manager.spark_bucket)

    logging.info("init products to s3 as parquet")
    file_name = ps.init_product_parquet_file()

    logging.info("uploading products from parquet to postgres")
    ps.upload_products_from_parquet_to_postgres(file_name)

if __name__ == "__main__":
    main()
