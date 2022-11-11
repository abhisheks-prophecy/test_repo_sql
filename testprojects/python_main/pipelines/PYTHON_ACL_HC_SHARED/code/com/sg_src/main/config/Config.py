from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, c_dbsecrets: str=None, c_string: str=None):
        self.spark = None
        self.update(c_dbsecrets, c_string)

    def update(self, c_dbsecrets: str="qasecrets_mysql:username", c_string: str="test"):

        if c_dbsecrets is not None:
            self.c_dbsecrets = self.get_dbutils(self.spark).secrets.get(*c_dbsecrets.split(":"))

        self.c_string = c_string
        pass
