from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(
            self,
            c_string: str=None, 
            c_long: int=None, 
            c_dbsecrets: str=None, 
            c_spark_expression: str=None, 
            c_float: float=None, 
            c_boolean: bool=None
    ):
        self.spark = None
        self.update(c_string, c_long, c_dbsecrets, c_spark_expression, c_float, c_boolean)

    def update(
            self,
            c_string: str="sdasd", 
            c_long: int=109, 
            c_dbsecrets: str="qasecrets_mysql:username", 
            c_spark_expression: str="concat('a', first_name)", 
            c_float: float=-12312.123, 
            c_boolean: bool=False
    ):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.c_string = c_string
        self.c_long = self.get_int_value(c_long)

        if c_dbsecrets is not None:
            self.c_dbsecrets = self.get_dbutils(prophecy_spark_context).secrets.get(*c_dbsecrets.split(":"))

        self.c_spark_expression = c_spark_expression
        self.c_float = self.get_float_value(c_float)
        self.c_boolean = self.get_bool_value(c_boolean)
        pass
