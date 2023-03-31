from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, test: bool=None, **kwargs):
        self.spark = None
        self.update(test)

    def update(self, test: bool=True, **kwargs):
        prophecy_spark = self.spark
        self.test = self.get_bool_value(test)
        pass
