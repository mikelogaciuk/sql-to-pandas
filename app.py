from config import APP_NAME, APP_BUILD
from config import FAKE_SQL_HOST, FAKE_SQL_DB
from config import FAKE_ORA_USR, FAKE_ORA_PWD, FAKE_ORA_SID, FAKE_ORA_PORT
from config import yesterday, last_month
from loguru import logger
from sqltopandas import SqlServerConnection, SqlServerAlternativeConnection, OracleConnection

logger.add(f"./{APP_NAME}.log", level="DEBUG", rotation="500 MB")

"""
As there is no additional database for this package.

You need to use your imagination...
"""

mssql_connection = SqlServerConnection(FAKE_SQL_HOST, FAKE_SQL_DB)
stores_list = mssql_connection.query("""SELECT stores FROM dbo.StoresSetup_V WHERE isClosed != 1""")

for i, row in stores_list.iterrows():
    store = row["store"]
    host_name = f"{store}avroce_ora"

    store_connection = OracleConnection(host_name, FAKE_ORA_USR, FAKE_ORA_PWD, FAKE_ORA_SID, FAKE_ORA_PORT)
    store_data_df = store_connection.query(f"""SELECT 
                               store, sale_id, sale_date, sale_number, sale_value, sale_tax 
                               FROM sales
                               WHERE sale_date >= {yesterday}
                               """)

    logger.info(f"Fetched: {len(store_data_df)} rows for store: {store}.")

    mssql_connection.push_data(store_data_df, "dbo.Sales", "staging")

    logger.info(f"Data pushed into: {FAKE_SQL_HOST} in staging of: {FAKE_SQL_DB}")
