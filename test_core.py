from config import FAKE_SQL_HOST, FAKE_SQL_DB
from config import FAKE_ORA_USR, FAKE_ORA_PWD, FAKE_ORA_SID, FAKE_ORA_PORT
from sqltopandas import SqlServerConnection, SqlServerAlternativeConnection, OracleConnection
import pytest


def test_sqlserver_engine_creation():

    test_mssql_connection = SqlServerConnection(FAKE_SQL_HOST, FAKE_SQL_DB)

    assert str(
        test_mssql_connection.engine) == f"""Engine(mssql+pymssql://{FAKE_SQL_HOST}/{FAKE_SQL_DB})"""


def test_ora_sql_engine_creation():

    store: str = "XS01"
    store_hostname: str = f"{store}avroce_ora"

    test_ora_connection = OracleConnection(
        store_hostname, FAKE_ORA_USR, FAKE_ORA_PWD, FAKE_ORA_SID, FAKE_ORA_PORT)
    assert str(
        test_ora_connection.engine) == f"""Engine(oracle+cx_oracle://{FAKE_ORA_USR}:***@{store_hostname}:{FAKE_ORA_PORT}/?service_name={FAKE_ORA_SID})"""
