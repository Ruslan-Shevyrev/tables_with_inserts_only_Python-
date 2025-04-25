import os
from configparser import ConfigParser

import vault_access_lib.module as val

URL_CONF = 'config/config.ini'


def get_os_variable(name, config_param=None, config_name='db', default_value=None):
    """
    Получает значение переменной окружения или из конфигурационного файла.
    :param name: Имя переменной.
    :param config_param: Имя параметра в конфиге. По умолчанию = name
    :param config_name: Имя раздела в конфиге.
    :param default_value: Значение по умолчанию
    :return: Значение переменной.
    """
    if config_param is None:
        config_param = name
    try:
        return os.environ[name]
    except KeyError:
        config = ConfigParser()
        config.read(URL_CONF)
        try:
            return config[config_name][config_param]
        except KeyError as exc:
            if not default_value:
                raise exc
            return default_value


def get_vault_variable(vault_obj,
                       secret_name: str,
                       secret_path: str,
                       config_param=None,
                       config_name='db', ):
    """
    Получает значение переменной из vault или из конфигурационного файла.
    :param vault_obj: Объект волта.
    :param secret_name: Название секрета
    :param secret_path: Путь до секрета в Волте
    :param config_param: Имя параметра в конфиге. По умолчанию = name
    :param config_name: Имя раздела в конфиге.
    :return: Значение переменной.
    """
    if config_param is None:
        config_param = secret_name
    value = vault_obj.get_secret(secret_name,
                                 secret_path)
    if not value:
        config = ConfigParser()
        config.read(URL_CONF)
        value = config[config_name][config_param]
    return value


VAULT_URL = get_os_variable('VAULT_URL')
VAULT_CERT_PATH = get_os_variable('VAULT_CERT_PATH')

LOKI_URL = get_os_variable('LOKI_URL')
LOKI_JOB_NAME = get_os_variable('LOKI_JOB_NAME')

vault = val.Vault("kafka-tables-insert-consumer",
                  VAULT_CERT_PATH,
                  VAULT_URL,
                  LOKI_URL)

APEX_USER = get_vault_variable(vault,
                               'db_login',
                               '',
                               'APEX_USER')

APEX_PASSWORD = get_vault_variable(vault,
                                   'db_password',
                                   '',
                                   'APEX_PASSWORD')

APEX_DSN = get_os_variable('APEX_DSN', 'APEX_DSN')

KAFKA_BOOTSTRAP_SERVER = get_os_variable('KAFKA_BOOTSTRAP_SERVER', 'KAFKA_BOOTSTRAP_SERVER')

TOPICS = ['TABLES_WITH_INSERTS_ONLY']

GROUP_ID = 'TABLES_WITH_INSERTS_ONLY_CONSUMER'
