import logging
import data_warehouse.star.star as star

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    star.setup_database()
    star.insert_products()
    star.insert_customers()
    star.insert_sales() 