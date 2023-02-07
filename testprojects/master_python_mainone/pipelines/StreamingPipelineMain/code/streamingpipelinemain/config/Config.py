from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, c_string: str=None):
        self.spark = None
        self.update(c_string)

    def update(self, c_string: str="this is test string"):
        self.c_string = c_string
        pass
