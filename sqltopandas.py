from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text
from dataclasses import dataclass, field
from config import DOMAIN_NAME, DOMAIN_USR, DOMAIN_PWD
from typing import Any
from loguru import logger
import pandas as pd
import socket
import sys


is_windows = sys.platform.startswith("win")


@dataclass
class SqlServerConnection:
    """Creates SQL Server Database Connection.

    Class used to create database connection with Microsoft SQL Server that builds engine depending on OS you are.
    Allows to use direct queries or fetch whole tables and export them further according to your needs.

    Attributes:

        host_name: str = String that determines host to connect into.
        database: str = String that determines database to connect into.
    """

    host_name: str
    database: str
    engine: str = field(init=False, repr=False)

    def __post_init__(self):
        if is_windows is True:
            temp_engine = f"mssql+pymssql://{self.host_name}/{self.database}"
            self.engine = create_engine(temp_engine)
        if is_windows is False:
            if (DOMAIN_NAME, DOMAIN_USR, DOMAIN_PWD, self.host_name, self.database) is not None:
                temp_engine = f"mssql+pymssql://{DOMAIN_NAME}\\{DOMAIN_USR}:{DOMAIN_PWD}@{self.host_name}/{self.database}"
                self.engine = create_engine(temp_engine)
            else:
                val = {
                    f"DOMAIN": DOMAIN_NAME,
                    f"DOMAIN_USR": DOMAIN_USR,
                    f"DOMAIN_PWD": DOMAIN_PWD,
                    f"HOSTNAME": self.host_name,
                    f"DATABASE": self.database
                }

                checker = {
                    key: value for (
                        key, value) in val.items() if value is None}
                logger.critical(
                    f"Check your environment variables, found 'None' values in: {checker}. Make sure that you have "
                    f"them in ~/.profile.")

    def resolve_host(self):
        try:
            socket.gethostbyname(self.host_name)
            return 1
        except socket.error:
            return 0

    def query(self, database_query: str):
        try:
            if self.resolve_host() == 1:
                query_result = pd.read_sql(database_query, self.engine)
                self.engine.dispose()
                return query_result
            elif self.resolve_host() == 0:
                logger.warning(
                    "Error while resolving hostname: {}".format(
                        self.host_name))
        except SQLAlchemyError as err:
            logger.warning(err)

    def fetch_table(self, table_name):
        try:
            if self.resolve_host() == 1:
                temp = pd.read_sql_table(table_name, self.engine)
                self.engine.dispose()
                return temp
            elif self.resolve_host() == 0:
                logger.warning(
                    "Error while resolving hostname: {}".format(
                        self.host_name))
        except SQLAlchemyError as err:
            logger.warning(err)

    def push_data(self, data_source, data_target: str, database_schema: str):
        try:
            if self.resolve_host() == 1:
                data_source.to_sql(
                    data_target,
                    con=self.engine,
                    schema=database_schema,
                    if_exists='append',
                    index=False)
            elif self.resolve_host() == 0:
                logger.warning(
                    "Error while resolving hostname: {}".format(
                        self.host_name))
        except SQLAlchemyError as err:
            logger.warning(err)

    def update_data(self, query_to_execute):
        try:
            if self.resolve_host() == 1:
                with Session(self.engine) as session:
                    result = session.execute(text(query_to_execute))
                    session.commit()
            elif self.resolve_host() == 0:
                logger.warning(
                    "Error while resolving hostname: {}".format(
                        self.host_name))
        except SQLAlchemyError as err:
            logger.warning(err)


@dataclass
class SqlServerAlternativeConnection(SqlServerConnection):
    """Creates SQL Server Database Connection with servers with named instance.

    Alternative for SqlServerConnection, that allows to fetch data or tables into dataframes push them further.

    Attributes:

        host_name: str = String that determines host to connect into.
        database: str = String that determines database to connect into.
        instance: str = String that determines a named instance to connect into.

    Note:

        Use especially for named instances (when SqlServerConnection fails to).
    """

    host_name: str
    database: str
    engine: str = field(init=False, repr=False)
    instance: str

    def __post_init__(self):
        if is_windows is True:
            temp_engine = f"mssql+pymssql://{self.host_name}\\{self.instance}/{self.database}"
            self.engine = create_engine(temp_engine)
        if is_windows is False:
            if (DOMAIN_NAME, DOMAIN_USR, DOMAIN_PWD, self.host_name, self.database, self.instance) is not None:
                temp_engine = f"""mssql+pymssql://{DOMAIN_NAME}\\{DOMAIN_USR}:{DOMAIN_PWD}@{self.host_name}\\{self.instance}/{self.database} """
                self.engine = create_engine(temp_engine)
            else:
                val = {
                    f"DOMAIN": DOMAIN_NAME,
                    f"USER": DOMAIN_USR,
                    f"PASSWORD": DOMAIN_PWD,
                    f"INSTANCE": self.instance,
                    f"HOSTNAME": self.host_name,
                    f"DATABASE": self.database
                }

                checker = {
                    key: value for (
                        key, value) in val.items() if value is None}
                logger.critical(
                    f"Check your environment variables, found 'None' values in: {checker}. Make sure that you have "
                    f"them in ~/.profile.")


@dataclass
class OracleConnection(SqlServerConnection):
    """Creates Oracle SQL Server Database Connection.

    Class used to create database connection with Oracle SQL Server.
    Allows to use direct queries or fetch whole tables and export them further.
    Inherits most of the code from SqlServerConnection.

    Attributes:

        host_name: str = String that determines host to connect into.
        host_user: str = String that determines database user to connect with to.
        host_password: str = String that determines database password to connect with to.
        host_service: str = String that determines database service to connect with to.
        host_port: int = Integer that determines database port to connect with to.
    """

    host_name: str
    database: str = field(init=False)
    host_user: str
    host_password: str
    host_service: str
    host_port: int
    engine: str = field(init=False, repr=False)

    def __post_init__(self):
        if (self.host_name, self.host_user, self.host_password, self.host_service, self.host_port) is not None:
            self.engine = create_engine(
                f"oracle+cx_oracle://{self.host_user}:{self.host_password}@{self.host_name}:{self.host_port}/?service_name={self.host_service}",
                connect_args={"events": True}, arraysize=1000, pool_timeout=5, pool_size=5, pool_pre_ping=True)
        else:
            val = {
                f"HOST": self.host_name,
                f"USER": self.host_user,
                f"PASSWORD": self.host_password,
                f"SERVICE": self.host_service,
                f"PORT": self.host_port
            }

            checker = {
                key: value for (
                    key, value) in val.items() if value is None}
            logger.critical(
                f"Check your environment variables, found 'None' values in: {checker}. Make sure that you have ")
