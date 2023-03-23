from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, coin_api_key: str=None, **kwargs):
        self.spark = None
        self.update(coin_api_key)

    def update(self, coin_api_key: str="abhinav_test:coin_api_key", **kwargs):
        prophecy_spark = self.spark

        if coin_api_key is not None:
            self.coin_api_key = self.get_dbutils(prophecy_spark).secrets.get(*coin_api_key.split(":"))

        pass
