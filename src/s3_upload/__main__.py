import logging
import s3_upload.sql_service as sql
import s3_upload.product_service as ps
from s3_upload.s3_manager import s3manager


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.getLogger("pyiceberg").setLevel(logging.DEBUG)

    logging.info("creating tables in postgres")
    sql.init_database()

    logging.info("init s3 bucket")
    s3manager.init_bucket()

    logging.info("uploading products to s3 as parquet")
    ps.upload_products_from_sql_to_s3_as_parquet()

    logging.info("uploading products to iceberg")
    ps.upload_products_from_sql_to_iceberg()

if __name__ == "__main__":
    main()
