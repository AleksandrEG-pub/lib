import os
import hashlib
import logging
import pandas as pd
import database.database_setup as db_setup
from database.database_connection import db

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

#CSV
data_products_file = f"{script_dir}/csv/products.csv"
data_customers_file = f"{script_dir}/csv/customers.csv"
data_sales_file = f"{script_dir}/csv/sales.csv"

# SQL, DDL
star_scheme_file = f"{script_dir}/sql/init_star.sql"
sales_pre_scheme_file = f"{script_dir}/sql/init_sales_pre.sql"

# SQL, DML
insert_to_star_sales = f"{script_dir}/sql/insert_to_star_sales.sql"
clean_sales_pre_file = f"{script_dir}/sql/clean_sales_pre.sql"


def setup_database() -> bool:
    logging.info('setting up database')
    db_setup.execute_scripts(star_scheme_file)
    db_setup.execute_scripts(sales_pre_scheme_file)
    logging.info("tables set up")
    

def insert_products():
    df = pd.read_csv(data_products_file)
    update_scd = f"""
        update product_dim d
            set end_date = now() - interval '1 hour',
            is_current = false
        where d.product_id = %s
          and d.is_current = true
          and d.product_hash <> %s;
    """
    insert_new = f"""
    insert into product_dim (
        product_hash,
        product_id,
        product_name,
        category,
        start_date,
        end_date,
        is_current
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    inserted_count = 0
    error_count = 0
    for index, row in df.iterrows():
        fields = ['product_id', 'product_name', 'category']
        data_string = ",".join(str(row[field]) for field in fields)
        hashstr = hashlib.md5(data_string.encode('utf-8')).hexdigest()
        values_update_scd = (
            str(row['product_id']),
            hashstr
        )
        values_insert_new = (
            hashstr,
            str(row['product_id']),
            str(row['product_name']),
            str(row['category']),
            'now()',
            '9999-12-31',
            'true'
        )
        try:
            with db.cursor() as cursor:
                cursor.execute(update_scd, values_update_scd)
                cursor.execute(insert_new, values_insert_new)
            inserted_count += 1       
        except Exception as e:
            error_count += 1
            logging.error(f"Error insering row {index}: {e}")
            continue
    logging.info(f"Insertion products complete. Success: {inserted_count}, Errors: {error_count}")
    
    
def insert_customers():
    df = pd.read_csv(data_customers_file)
    update_scd = f"""
        update customer_dim cd
            set end_date = now() - interval '1 hour',
            is_current = false
        where cd.customer_id = %s
          and cd.is_current = true
          and cd.customer_hash <> %s;
    """
    insert_new = f"""
    insert into customer_dim (
        customer_hash,
        customer_id,
        name,
        email,
        phone,
        start_date,
        end_date,
        is_current
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    inserted_count = 0
    error_count = 0
    for index, row in df.iterrows():
        fields = ['customer_id', 'name', 'email', 'phone']
        data_string = ",".join(str(row[field]) for field in fields)
        hashstr = hashlib.md5(data_string.encode('utf-8')).hexdigest()
        values_update_scd = (
            str(row['customer_id']),
            hashstr
        )
        values_insert_new = (
            hashstr,
            str(row['customer_id']),
            str(row['name']),
            str(row['email']),
            str(row['phone']),
            'now()',
            '9999-12-31',
            'true'
        )
        try:
            with db.cursor() as cursor:
                cursor.execute(update_scd, values_update_scd)
                cursor.execute(insert_new, values_insert_new)
            inserted_count += 1       
        except Exception as e:
            error_count += 1
            logging.error(f"Error insering row {index}: {e}")
            continue
    logging.info(f"Insertion customers complete. Success: {inserted_count}, Errors: {error_count}")

    
def insert_sales():
    try:
        _load_sales_from_csv()
        db_setup.execute_scripts(insert_to_star_sales)
    finally:
        db_setup.execute_scripts(clean_sales_pre_file)


def _load_sales_from_csv():
    table_name = 'sales_pre'
    columns = "(sale_id, customer_id, product_id, amount, quantity, date_value)"
    with db.cursor() as cursor:
        try:
            with open(data_sales_file, 'r') as f:
                logging.info(
                    f"Inserting data from {data_sales_file} into {table_name}")
                cursor.copy_expert(
                    f"COPY {table_name}{columns} FROM STDIN WITH CSV HEADER", f)
                logging.info(
                    f"Successfully inserted data into {table_name}")
        except Exception as e:
            logging.error(f"Error inserting data into {table_name}: {e}")
            raise
