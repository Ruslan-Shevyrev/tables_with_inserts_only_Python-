import json
import time
import oracledb

from typing import Dict
from kafka import KafkaConsumer
from loki_logging_lib import loki_handler
from oracle_connector import OracleDB

from settings import *

logger = loki_handler.setup_logger(loki_url=LOKI_URL, service_name=LOKI_JOB_NAME)

oracle_pool = OracleDB(user=APEX_USER, password=APEX_PASSWORD, dsn=APEX_DSN, logger=logger)


def get_sql_scripts() -> Dict[str, str]:
    """
    Загружает SQL-скрипты из базы данных.
    :return: Словарь с кодами и текстами SQL-скриптов.
    """
    sqls = {}
    for code, script in oracle_pool.execute_query_and_fetchall("""
        SELECT s.CODE, s.SCRIPT 
        FROM APP_APEX_MICROSERVICES.V_PYTHON_SQL s 
        WHERE s.PROJECT_NAME = 'tables_with_inserts_only' 
        ORDER BY s.ID
    """):
        sqls[str(code)] = script.read()
    logger.info('Получение SQL-скриптов из Oracle.')
    return sqls


def main():
    try:
        # Загрузка SQL-скриптов
        sql_list = get_sql_scripts()

        # Подключение к Kafka
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False
        )
        consumer.subscribe(TOPICS)

        for message in consumer:
            try:
                consumer.commit()
            except Exception as e:
                logger.warning('Kafka commit error, continue')
                time.sleep(1)
                continue

            json_message = json.loads(message.value.decode('utf-8'))
            dbid = json_message.get('DBID')
            rows_db = []
            error_text = None
            if dbid is not None:
                rows = oracle_pool.execute_query_and_fetchall(
                                sql_list['TABLES_WITH_INSERTS_ONLY_SELECT_DB_PASS_DSN'],
                                dbid=dbid)

                if len(rows) == 1:
                    oracle_pool.execute_query_and_commit(
                            sql_list['TABLES_WITH_INSERTS_ONLY_START_DB'],
                            dbid=dbid)
                    logger.info(f"started DB: {str(dbid)}")

                for row in rows:
                    if len(rows) == 1:
                        try:
                            with oracledb.connect(user="SYS",
                                                  password=row[0],
                                                  dsn=row[1],
                                                  mode=oracledb.SYSDBA) as connection_db:
                                with connection_db.cursor() as cursor_db:
                                    cursor_db.execute(sql_list['TABLES_WITH_INSERTS_ONLY_SELECT_RESULTS'])
                                    rows_db = cursor_db.fetchall()
                                    status = 'success'
                        except Exception as e:
                            rows_db.clear()
                            status = 'error'
                            try:
                                error_text = str(e)
                            except Exception as ex:
                                error_text = 'Unknown Error'

                        for row_db in rows_db:
                            oracle_pool.execute_query_and_commit(
                                                    sql_list['TABLES_WITH_INSERTS_ONLY_INSERT_RESULTS'],
                                                    dbid=dbid,
                                                    table_owner=row_db[0],
                                                    table_name=row_db[1],
                                                    total_inserts=row_db[2],
                                                    total_updates=row_db[3],
                                                    total_deletes=row_db[4],
                                                    size_mb=row_db[5],
                                                    table_partitioned=row_db[6],
                                                    table_compress=row_db[7],
                                                    part_compress=row_db[8])

                            oracle_pool.execute_query_and_commit(
                                                sql_list['TABLES_WITH_INSERTS_ONLY_INSERT_RES_CONN'],
                                                dbid=dbid,
                                                status=str(status),
                                                error_text=error_text)
                            logger.info(f"finished DB: {str(dbid)}")

    except Exception as e:
        logger.critical(f"Critical error occurred: {str(e)}")


main()
