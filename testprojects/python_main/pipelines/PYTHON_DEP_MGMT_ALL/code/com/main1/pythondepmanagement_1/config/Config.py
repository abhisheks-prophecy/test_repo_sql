from com.main1.pythondepmanagement_1.graph.SubGraph_2.config.Config import SubgraphConfig as SubGraph_2_Config
from com.main1.pythondepmanagement_1.graph.all_type_main_pythonsg.config.Config import (
    SubgraphConfig as all_type_main_pythonsg_Config
)
from com.main1.pythondepmanagement_1.graph.SubGraph_7.config.Config import SubgraphConfig as SubGraph_7_Config
from com.main1.pythondepmanagement_1.graph.Subgraph_2.config.Config import SubgraphConfig as Subgraph_2_Config
from com.main1.pythondepmanagement_1.graph.Subgraph_1.config.Config import SubgraphConfig as Subgraph_1_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            Subgraph_1: dict=None,
            SubGraph_2: dict=None,
            all_type_main_pythonsg: dict=None,
            Subgraph_2: dict=None,
            SubGraph_7: dict=None,
            **kwargs
    ):
        self.spark = None
        self.update(Subgraph_1, SubGraph_2, all_type_main_pythonsg, Subgraph_2, SubGraph_7)

    def update(
            self,
            Subgraph_1: dict={},
            SubGraph_2: dict={},
            all_type_main_pythonsg: dict={},
            Subgraph_2: dict={},
            SubGraph_7: dict={},
            **kwargs
    ):
        prophecy_spark = self.spark
        self.Subgraph_1 = self.get_config_object(
            prophecy_spark, 
            Subgraph_1_Config(prophecy_spark = prophecy_spark), 
            Subgraph_1, 
            Subgraph_1_Config
        )
        self.SubGraph_2 = self.get_config_object(
            prophecy_spark, 
            SubGraph_2_Config(prophecy_spark = prophecy_spark), 
            SubGraph_2, 
            SubGraph_2_Config
        )
        self.all_type_main_pythonsg = self.get_config_object(
            prophecy_spark, 
            all_type_main_pythonsg_Config(prophecy_spark = prophecy_spark), 
            all_type_main_pythonsg, 
            all_type_main_pythonsg_Config
        )
        self.Subgraph_2 = self.get_config_object(
            prophecy_spark, 
            Subgraph_2_Config(prophecy_spark = prophecy_spark), 
            Subgraph_2, 
            Subgraph_2_Config
        )
        self.SubGraph_7 = self.get_config_object(
            prophecy_spark, 
            SubGraph_7_Config(prophecy_spark = prophecy_spark), 
            SubGraph_7, 
            SubGraph_7_Config
        )
        pass
