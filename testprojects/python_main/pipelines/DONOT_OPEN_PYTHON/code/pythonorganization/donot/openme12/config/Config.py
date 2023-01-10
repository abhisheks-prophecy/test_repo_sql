from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            JDBC_URL: str=None, 
            JDBC_SOURCE_TABLE: str=None, 
            CONFIG_STR: str=None, 
            CONFIG_BOOLEAN: bool=None, 
            CONFIG_DOUBLE: float=None, 
            CONFIG_INT: int=None, 
            CONFIG_FLOAT: float=None, 
            CONFIG_SHORT: int=None, 
            CONFIG_DB_SECRETS: str=None
    ):
        self.spark = None
        self.update(
            JDBC_URL, 
            JDBC_SOURCE_TABLE, 
            CONFIG_STR, 
            CONFIG_BOOLEAN, 
            CONFIG_DOUBLE, 
            CONFIG_INT, 
            CONFIG_FLOAT, 
            CONFIG_SHORT, 
            CONFIG_DB_SECRETS
        )

    def update(
            self,
            JDBC_URL: str="jdbc:mysql://18.144.156.219:3306/test_database", 
            JDBC_SOURCE_TABLE: str="test_table", 
            CONFIG_STR: str=None, 
            CONFIG_BOOLEAN: bool=True, 
            CONFIG_DOUBLE: float=1.00123211232E7, 
            CONFIG_INT: int=None, 
            CONFIG_FLOAT: float=4567546.5, 
            CONFIG_SHORT: int=120, 
            CONFIG_DB_SECRETS: str="qasecrets:mysql_user"
    ):
        self.JDBC_URL = JDBC_URL
        self.JDBC_SOURCE_TABLE = JDBC_SOURCE_TABLE
        self.CONFIG_STR = CONFIG_STR
        self.CONFIG_BOOLEAN = self.get_bool_value(CONFIG_BOOLEAN)
        self.CONFIG_DOUBLE = self.get_float_value(CONFIG_DOUBLE)
        self.CONFIG_INT = self.get_int_value(CONFIG_INT)
        self.CONFIG_FLOAT = self.get_float_value(CONFIG_FLOAT)
        self.CONFIG_SHORT = self.get_int_value(CONFIG_SHORT)

        if CONFIG_DB_SECRETS is not None:
            self.CONFIG_DB_SECRETS = self.get_dbutils(self.spark).secrets.get(*CONFIG_DB_SECRETS.split(":"))

        pass
