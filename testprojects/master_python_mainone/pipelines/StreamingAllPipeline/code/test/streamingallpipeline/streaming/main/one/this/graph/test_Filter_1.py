from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from streamingallpipeline.streaming.main.one.this.graph.Filter_1 import *
import streamingallpipeline.streaming.main.one.this.config.ConfigStore as ConfigStore


class Filter_1Test(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/streamingallpipeline/streaming/main/one/this/graph/Filter_1/in0/schema.json',
            'test/resources/data/streamingallpipeline/streaming/main/one/this/graph/Filter_1/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/streamingallpipeline/streaming/main/one/this/graph/Filter_1/out/schema.json',
            'test/resources/data/streamingallpipeline/streaming/main/one/this/graph/Filter_1/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = Filter_1(self.spark, dfIn0)
        assertDFEquals(dfOut.select("c_bigint"), dfOutComputed.select("c_bigint"), self.maxUnequalRowsToShow)

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        ConfigStore.Utils.initializeFromArgs(
            self.spark,
            Namespace(file = f"configs/resources/config/{fabricName}.json", config = None)
        )