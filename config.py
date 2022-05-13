"""
Main config file
"""

# Core variables
import keyring
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

APP_NAME: str = "SQL-TO-PANDAS"
APP_BUILD: float = 0.1

FAKE_SQL_HOST: str = "avroce.ec2.dl"
FAKE_SQL_DB: str = "SALES_DB"

FAKE_ORA_PWD: str = keyring.get_password("fakeora", "pwd")
FAKE_ORA_USR: str = "store_user"
FAKE_ORA_SID: str = "store_db"
FAKE_ORA_PORT: int = 6666

# Supplementary variables
yesterday: str = str((date.today()) - timedelta(days=1))
last_three_days: str = str((date.today()) - timedelta(days=3))
last_seven_days: str = str((date.today()) - timedelta(days=7))
last_half_month: str = str((date.today()) - timedelta(days=14))
last_month: str = str((date.today()) - relativedelta(months=+1))
last_quarter: str = str((date.today()) - relativedelta(months=+3))
today: str = str((date.today()))
