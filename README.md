# sql-to-pandas

## About

Three simple classes that makes connection to `Microsoft SQL Server` and `Oracle SQL Server` little easier.

Especially when you need to fetch data directly into `Pandas dataframe` for data wrangling.

Classes use `loguru` for logging and printing purposes. This can be easily edited into print, or removed.

## Installation

Clone repo:

```shell
mkdir sql-to-pandas
cd sql-to-pandas
git clone https://github.com/mikelogaciuk/sql-to-pandas.git
pip install -r requirements.txt
```

## Example usage

You can used like below:

```py
from config import APP_NAME, APP_BUILD
from config import FAKE_SQL_HOST, FAKE_SQL_DB
from config import FAKE_ORA_USR, FAKE_ORA_PWD, FAKE_ORA_SID, FAKE_ORA_PORT
from config import yesterday, last_month
from loguru import logger
import sql2pandas as xd

logger.add(f"./{APP_NAME}.log", level="DEBUG", rotation="500 MB")

mssql_connection = xd.SqlServerConnection(FAKE_SQL_HOST, FAKE_SQL_DB)
stores_list = mssql_connection.query("""SELECT stores FROM dbo.StoresSetup_V WHERE isClosed != 1""")

for i, row in stores_list.iterrows():
    store = row["store"]
    host_name = f"{store}avroce_ora"

    store_connection = xd.OracleConnection(host_name, FAKE_ORA_USR, FAKE_ORA_PWD, FAKE_ORA_SID, FAKE_ORA_PORT)
    store_data_df = store_connection.query(f"""SELECT 
                               store, sale_id, sale_date, sale_number, sale_value, sale_tax 
                               FROM sales
                               WHERE sale_date >= {yesterday}
                               """)

    logger.info(f"Fetched: {len(store_data_df)} rows for store: {store}.")

    mssql_connection.push_data(store_data_df, "dbo.Sales", "staging")

    logger.info(f"Data pushed into: {FAKE_SQL_HOST} in staging of: {FAKE_SQL_DB}")
```

## License

MIT License

Copyright (c) 2021 Mike Logaciuk

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.