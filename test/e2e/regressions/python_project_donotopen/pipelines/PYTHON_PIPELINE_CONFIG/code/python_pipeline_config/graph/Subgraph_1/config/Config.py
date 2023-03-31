from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(self, prophecy_spark=None, c_string: str="asdasd", c_int: str="10", c_float: str="12.12", **kwargs):
        self.c_string = c_string
        self.c_int = c_int
        self.c_float = c_float
        pass

    def update(self, updated_config):
        self.c_string = updated_config.c_string
        self.c_int = updated_config.c_int
        self.c_float = updated_config.c_float
        pass

Config = SubgraphConfig()
