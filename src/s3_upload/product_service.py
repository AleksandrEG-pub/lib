import logging
import pyarrow as pa
from database.adbc_connection import adbc_connect
from s3_upload.s3 import s3manager

from pyiceberg.catalog import Catalog
from pyiceberg.table import Table
import s3_upload.iceberg_service as i_serv
import pyarrow as pa
import pyarrow.compute as pa_compute


def _convert_numeric_to_decimal(arrow_table: pa.Table) -> pa.Table:
    """
    PyArrow does not convert postgres decimal as pyarrow decimal, instead it wraps type with type-wrapper
    Convert PostgreSQL numeric extension to Arrow decimal.
    """
    for i, field in enumerate(arrow_table.schema):
        if (hasattr(field.type, 'type_name') and field.type.type_name == 'numeric'):
            column = arrow_table.column(i)
            decimal_column = pa_compute.cast(column, pa.decimal128(13, 2))
            arrow_table = arrow_table.set_column(
                i, field.name, decimal_column
            )
    return arrow_table


def _load_products() -> pa.Table:
    with adbc_connect.cursor() as cursor:
        cursor.execute("SELECT * FROM products")
        arrow_table: "pa.Table" = cursor.fetch_arrow_table()
        arrow_table = _convert_numeric_to_decimal(arrow_table)
        return arrow_table


def upload_products_from_sql_to_s3_as_parquet():
    pa_table: pa.Table = _load_products()
    file = 'products.parquet'
    s3manager.write_as_parquet(s3manager.bucket, file, pa_table)
    logging.info(f"product data written to file [{file}] in bucket [{s3manager.bucket}]")


def upload_products_from_sql_to_iceberg():
    catalog_name = "store"
    namespace = "main"
    table_name = "products"
    bucket_name = s3manager.bucket

    logging.info("getting catalog")
    catalog: Catalog = i_serv.get_or_create_catalog_s3_sql(
        catalog_name, bucket_name)
    products_pa_table: pa.Table = _load_products()
    logging.info("getting table")
    iceberg_table: Table = i_serv.get_or_create_table(
        table_name=table_name,
        namespace=namespace,
        catalog=catalog,
        schema=products_pa_table.schema,
    )
    logging.info("adding data")
    iceberg_table.append(products_pa_table)
    logging.info(f"product data written to iceberg catalog [{catalog.name}] in table [{iceberg_table.metadata.location}]")
