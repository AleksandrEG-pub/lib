import logging
import time
from database.clickhouse_connection import ch_db
from typing import Dict
from database.clickhouse_connection import ch_db
from clickhouse.query import queries_config


def setup(config: Dict):
    logging.info("enabling log")
    ch_db.execute_command(config['setup']['enable_query_log'])
    logging.info("creating log table")
    ch_db.execute_command(config['setup']['create_table'])
    logging.info("generating log data")
    ch_db.execute_command(config['setup']['insert_data'])


def run_webserver_log_analytic():
    logging.info("start analytic")
    config = queries_config.config()
    setup(config)
    
    logging.info("daily_request_count")
    """
    Uses primary key (timestamp, user_id)
    it is expected that most of the queries will be based on time and executor
    """
    ch_db.execute_query(config['queries']['daily_request_count'])
    
    logging.info("")
    logging.info("общее количество запросов за каждый день.")
    logging.info("Table scan, no optimization, existing index on timestamp does not work on grouping")
    ch_db.execute_query(config['queries']['daily_request_count'])
    logging.info("Optimization. Create a projection group by day 'day_projection' for collect dayly amounts of requests")
    ch_db.execute_command(config['optimizations']['drop_projection_day'])
    # instead of checking in a blocking way  
    # if any changes to database are made and complete
    # time.sleep(n) is introduced
    time.sleep(1)
    ch_db.execute_command(config['optimizations']['create_projection_day'])
    ch_db.execute_command(config['optimizations']['materialize_projection_day'])
    time.sleep(2)
    logging.info("after optimization. uses created projection 'day_projection'")
    ch_db.execute_query(config['queries']['daily_request_count'])
    
    logging.info("")
    logging.info("среднее время ответа для каждого URL.")
    logging.info("Table scan, no optimization. there is no index on 'url' column")
    ch_db.execute_query(config['queries']['avg_response_per_url'])
    logging.info("Optimization. Create a grouping by url projection 'avg_response_projection'")
    ch_db.execute_command(config['optimizations']['drop_projection_avg_response'])
    time.sleep(1)
    ch_db.execute_command(config['optimizations']['create_projection_avg_response'])
    ch_db.execute_command(config['optimizations']['materialize_projection_avg_response'])
    time.sleep(2)
    logging.info("uses created projection 'avg_response_projection'")
    ch_db.execute_query(config['queries']['avg_response_per_url'])
    
    logging.info("")
    logging.info("Количество запросов с ошибками (статус-коды 4xx и 5xx).")
    logging.info("Table scan, no optimization. there is not index on status_code column")
    ch_db.execute_query(config['queries']['error_counts'])
    logging.info("Create a materializaed view using SummingMergeTree engine, which will hold accumulating values.")
    ch_db.execute_command(config['optimizations']['drop_error_count_view'])
    time.sleep(1)
    ch_db.execute_command(config['optimizations']['create_error_count_view'])
    logging.info("uses created materialized projection 'error_counts_mv'")
    ch_db.execute_query(config['queries']['error_counts_view'])
    
    logging.info("")
    logging.info("топ-10 пользователей по количеству запросов. ")
    logging.info("Table scan, no optimization, existing 'user_id' in primary index is not used")
    ch_db.execute_query(config['queries']['top_users_by_requests'])
    logging.info("Optimization. Create a grouping by projection 'user_requests_count_projection'")
    ch_db.execute_command(config['optimizations']['drop_projection_avg_response'])
    time.sleep(1)
    ch_db.execute_command(config['optimizations']['create_projection_user_requests'])
    ch_db.execute_command(config['optimizations']['materialize_projection_user_requests'])
    time.sleep(2)
    logging.info("uses created projection 'user_requests_count_projection'")
    ch_db.execute_query(config['queries']['top_users_by_requests'])



