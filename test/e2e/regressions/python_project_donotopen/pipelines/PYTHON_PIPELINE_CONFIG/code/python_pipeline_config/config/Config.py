from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(self, c_string: str=None, c_int: str=None, c_float: str=None):
        self.spark = None
        self.update(c_string, c_int, c_float)

    def update(self, c_string: str="asdasd", c_int: str="10", c_float: str="12.12"):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.c_string = c_string
        self.c_int = c_int
        self.c_float = c_float
        pass
