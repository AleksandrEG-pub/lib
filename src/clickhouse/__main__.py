import logging
import clickhouse.log_analytic as la


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    la.run_webserver_log_analytic()
    