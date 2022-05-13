from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text
from dataclasses import dataclass, field
from typing import Any
from loguru import logger
import pandas as pd
import socket
import sys


@dataclass
class SqlServerConnection:
    """Creates SQL Server Database Connection.

    Class used to create database connection with Microsoft SQL Server.
    Allows to use direct queries or fetch whole tables and export them further.

    Args:

        host_name: str = String that determines host to connect into.
        database: str = String that determines database to connect into.

    Note:
        This will not work with named instances.
        Use SqlServerAlternativeConnection instead.

    Warning:
        Class works only with LDAP (Windows Authentication).
    """

    host_name: str
    database: str
    engine: Any = field(init=False, repr=False)

    def __post_init__(self):
        self.engine = create_engine(
            f"mssql+pymssql://{self.host_name}/{self.database}"
        )

    def resolve_host(self):
        is_windows = sys.platform.startswith("win")

        if is_windows is True:
            try:
                socket.gethostbyname(self.host_name)
                return 1
            except socket.error:
                return 0
        else:
            return 1

    def query(self, database_query: str):
        try:
            if self.resolve_host() == 1:
                query_result = pd.read_sql(database_query, self.engine)
                self.engine.dispose()
                return query_result
            elif self.resolve_host() == 0:
                logger.warning("Error while resolving hostname: {}".format(self.host_name))
        except SQLAlchemyError as err:
            logger.warning(err)

    def fetch_table(self, table_name):
        try:
            if self.resolve_host() == 1:
                temp = pd.read_sql_table(table_name, self.engine)
                self.engine.dispose()
                return temp
            elif self.resolve_host() == 0:
                logger.warning("Error while resolving hostname: {}".format(self.host_name))
        except SQLAlchemyError as err:
            logger.warning(err)

    def push_data(self, data_source, data_target: str, database_schema: str):
        try:
            if self.resolve_host() == 1:
                data_source.to_sql(data_target, con=self.engine, schema=database_schema, if_exists='append', index=False)
            elif self.resolve_host() == 0:
                logger.warning("Error while resolving hostname: {}".format(self.host_name))
        except SQLAlchemyError as err:
            logger.warning(err)

    def update_data(self, query_to_execute):
        try:
            if self.resolve_host() == 1:
                with Session(self.engine) as session:
                    result = session.execute(text(query_to_execute))
                    session.commit()
            elif self.resolve_host() == 0:
                logger.warning("Error while resolving hostname: {}".format(self.host_name))
        except SQLAlchemyError as err:
            logger.warning(err)


@dataclass
class SqlServerAlternativeConnection(SqlServerConnection):
    """Creates SQL Server Database Connection.

    Class used to create database connection with Microsoft SQL Server.
    Allows to use direct queries or fetch whole tables and export them further.

    Args:

        host_name: str = String that determines host to connect into.
        database: str = String that determines database to connect into.
        instance: str = String that determines a named instance to connect into.

    Note:

        Use especially for named instances (when SqlServerConnection fails to).

    Warning:
        Class works only with LDAP (Windows Authentication).
    """

    host_name: str
    database: str
    engine: Any = field(init=False, repr=False)
    instance: str

    def __post_init__(self):
        self.engine = create_engine(
            f"mssql+pymssql://{self.host_name}\\{self.instance}/{self.database}"
        )


@dataclass
class OracleConnection(SqlServerConnection):
    """Creates Oracle SQL Server Database Connection.

    Class used to create database connection with Oracle SQL Server.
    Allows to use direct queries or fetch whole tables and export them further.
    Inherits most code from SqlServerConnection.

    Args:

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
    engine: Any = field(init=False, repr=False)

    def __post_init__(self):
        self.engine = create_engine(
                    f"oracle+cx_oracle://{self.host_user}:{self.host_password}@{self.host_name}:{self.host_port}/?service_name={self.host_service}",
                    connect_args={"events": True}, arraysize=1000, pool_timeout=5, pool_size=5, pool_pre_ping=True)