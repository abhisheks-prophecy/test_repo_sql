from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def Subgraph_2(spark: SparkSession, config: SubgraphConfig, in0: DataFrame) -> None:
    Config.update(config)
