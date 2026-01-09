import pyiceberg.catalog as ice_cat
import pyiceberg.table as ice_table
import s3_upload.sql_service as sql
from s3_upload.s3_manager import s3manager


def get_or_create_catalog_s3_sql(catalog_name: str, bucket: str):
    database_url = f"{sql.get_database_url()}"
    catalog = ice_cat.load_catalog(
        catalog_name,
        **{
            "type": "sql",
            "uri": database_url,
            "warehouse": f"s3://{bucket}/icebearg_warehouse",
            "s3.endpoint": s3manager.endpoint,
            "s3.region": s3manager.region,
        }
    )
    return catalog


def get_or_create_table(table_name: str,
                        namespace: str,
                        catalog: ice_cat.Catalog,
                        schema) -> ice_table.Table:
    table_id = f"{namespace}.{table_name}"
    if catalog.table_exists(table_id):
        return catalog.load_table(table_id)
    catalog.create_namespace_if_not_exists(namespace)
    table = catalog.create_table_if_not_exists(
        identifier=table_id,
        schema=schema,
    )
    return table
