from python_pipeline_config.graph.Subgraph_1.config.Config import SubgraphConfig as Subgraph_1_Config
from python_pipeline_config.graph.PYTHON_SG.config.Config import SubgraphConfig as PYTHON_SG_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            c_string: str=None,
            c_int: str=None,
            c_float: str=None,
            Subgraph_1: dict=None,
            PYTHON_SG: dict=None,
            **kwargs
    ):
        self.spark = None
        self.update(c_string, c_int, c_float, Subgraph_1, PYTHON_SG)

    def update(
            self,
            c_string: str="asdasd",
            c_int: str="10",
            c_float: str="12.12",
            Subgraph_1: dict={},
            PYTHON_SG: dict={},
            **kwargs
    ):
        prophecy_spark = self.spark
        self.c_string = c_string
        self.c_int = c_int
        self.c_float = c_float
        self.Subgraph_1 = self.get_config_object(
            prophecy_spark, 
            Subgraph_1_Config(prophecy_spark = prophecy_spark), 
            Subgraph_1, 
            Subgraph_1_Config
        )
        self.PYTHON_SG = self.get_config_object(
            prophecy_spark, 
            PYTHON_SG_Config(prophecy_spark = prophecy_spark), 
            PYTHON_SG, 
            PYTHON_SG_Config
        )
        pass
