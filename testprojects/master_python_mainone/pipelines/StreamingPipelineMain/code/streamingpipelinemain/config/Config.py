from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(self, c_string: str=None):
        self.spark = None
        self.update(c_string)

    def update(self, c_string: str="this is test string"):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.c_string = c_string
        pass
