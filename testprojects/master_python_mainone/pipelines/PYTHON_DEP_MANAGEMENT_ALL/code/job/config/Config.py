from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            JDBC_URL: str=None, 
            JDBC_SOURCE_TABLE: str=None, 
            CONFIG_BOOLEAN: bool=None, 
            CONFIG_DOUBLE: float=None, 
            CONFIG_INT: int=None, 
            CONFIG_FLOAT: float=None, 
            CONFIG_SHORT: int=None, 
            CONFIG_DB_SECRETS: str=None, 
            CONFIG_STR: str=None, 
            c_0: int=None, 
            c_1: int=None, 
            c_row: str=None, 
            c_limit_45: int=None, 
            c_st_expr: str=None, 
            c_st_renamed: str=None, 
            c_sql_expr: str=None, 
            c_regex1: str=None, 
            c_regex2: str=None
    ):
        self.spark = None
        self.update(
            JDBC_URL, 
            JDBC_SOURCE_TABLE, 
            CONFIG_BOOLEAN, 
            CONFIG_DOUBLE, 
            CONFIG_INT, 
            CONFIG_FLOAT, 
            CONFIG_SHORT, 
            CONFIG_DB_SECRETS, 
            CONFIG_STR, 
            c_0, 
            c_1, 
            c_row, 
            c_limit_45, 
            c_st_expr, 
            c_st_renamed, 
            c_sql_expr, 
            c_regex1, 
            c_regex2
        )

    def update(
            self,
            JDBC_URL: str="jdbc:mysql://18.144.156.219:3306/test_database", 
            JDBC_SOURCE_TABLE: str="test_table", 
            CONFIG_BOOLEAN: bool=True, 
            CONFIG_DOUBLE: float=1.00123211232E7, 
            CONFIG_INT: int=None, 
            CONFIG_FLOAT: float=4567546.5, 
            CONFIG_SHORT: int=120, 
            CONFIG_DB_SECRETS: str="qasecrets:mysql_user", 
            CONFIG_STR: str=None, 
            c_0: int=0, 
            c_1: int=1, 
            c_row: str="row_number()", 
            c_limit_45: int=45, 
            c_st_expr: str="concat(`c   short  --`, `c-int-column type`)", 
            c_st_renamed: str="c-decimal renamed", 
            c_sql_expr: str="%1%", 
            c_regex1: str="^[_A-Za-z0-9-]+(\\\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\\\.[A-Za-z0-9]+)*(\\\\.[A-Za-z]{2,})", 
            c_regex2: str="((?=.*)(?=.*[a-z$$])(?=.*[A-Z])(?=.*[@#%]).{6,20})"
    ):
        self.JDBC_URL = JDBC_URL
        self.JDBC_SOURCE_TABLE = JDBC_SOURCE_TABLE
        self.CONFIG_BOOLEAN = self.get_bool_value(CONFIG_BOOLEAN)
        self.CONFIG_DOUBLE = self.get_float_value(CONFIG_DOUBLE)
        self.CONFIG_INT = self.get_int_value(CONFIG_INT)
        self.CONFIG_FLOAT = self.get_float_value(CONFIG_FLOAT)
        self.CONFIG_SHORT = self.get_int_value(CONFIG_SHORT)

        if CONFIG_DB_SECRETS is not None:
            self.CONFIG_DB_SECRETS = self.get_dbutils(self.spark).secrets.get(*CONFIG_DB_SECRETS.split(":"))

        self.CONFIG_STR = CONFIG_STR
        self.c_0 = self.get_int_value(c_0)
        self.c_1 = self.get_int_value(c_1)
        self.c_row = c_row
        self.c_limit_45 = self.get_int_value(c_limit_45)
        self.c_st_expr = c_st_expr
        self.c_st_renamed = c_st_renamed
        self.c_sql_expr = c_sql_expr
        self.c_regex1 = c_regex1
        self.c_regex2 = c_regex2
        pass
