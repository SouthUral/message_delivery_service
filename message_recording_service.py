"""
Сервис оценки качества соединения с устройствами
"""
__version__ = '0.0.1'

import json
import os
import re
import sys
import time

import psycopg2
import pymongo
import toml
from cryptography.fernet import Fernet
from loguru import logger

time_diff = {}


def get_decrypted(text):
    return Fernet(os.getenv('ASD_CIPHER_KEY').encode()).decrypt(text.encode()).decode()


def run_service():
    config = toml.load('./config.toml')

    logger.remove()
    logger.add(sys.stderr, format='<lvl>[{level} {time:DDMMYY} {time:HH:mm:ss} :{line}]</lvl> {message}')
    
    hosts = []
    hosts.append(f"{os.environ.get('ASD_MDB_HOST1')}:{int(os.environ.get('ASD_MDB_PORT1'))}")
    hosts.append(f"{os.environ.get('ASD_MDB_HOST2')}:{int(os.environ.get('ASD_MDB_PORT2'))}")

    mongo = pymongo.MongoClient(host=hosts,
                                username=config.get('mongo', {}).get('username'),
                                password=get_decrypted(config.get('mongo', {}).get('password')),
                                authSource='ilogic',
                                authMechanism='SCRAM-SHA-256',
                                replicaSet='rs0',
                                readPreference='primary',
                                )
    logger.success(f"Connected to MongoDB: {mongo.server_info()}")

    postgres = psycopg2.connect(user=config.get('postgres', {}).get('username'),
                                password=get_decrypted(config.get('postgres', {}).get('password')),
                                host=os.getenv('ASD_POSTGRES_HOST'),
                                port=int(os.getenv('ASD_POSTGRES_PORT')),
                                database=os.getenv('ASD_POSTGRES_DBNAME'))
    logger.success(f"Connected to PostgreSQL: {postgres.get_dsn_parameters()}")
    pg = postgres.cursor()

    try:
        logger.info('Сервис запущен')

        pipeline = [
            # {'$match': {'ns.coll': re.compile(r'^(device\.|\d+$)')}},
            {'$match': {'operationType': 'insert'}},
        ]
        with mongo.ilogic.watch(pipeline) as stream:
            start = time.time()
            processed = 0
            for change in stream:
                document = change.get('fullDocument')

                message = json.dumps(document, default=str)

                pg.execute('call sh_signal.calc_quality(%s)', (message,))
                postgres.commit()

                processed += 1
                if time.time() - start >= 1:
                    logger.info(f'{processed}')
                    processed = 0
                    start = time.time()

    except KeyboardInterrupt:
        logger.warning(f'Сервис завершает свою работу по прерыванию пользователя')
    except Exception:
        logger.exception('Произошла ошибка! Сервис завершает свою работу...')


if __name__ == '__main__':
    run_service()
    sys.exit(1)