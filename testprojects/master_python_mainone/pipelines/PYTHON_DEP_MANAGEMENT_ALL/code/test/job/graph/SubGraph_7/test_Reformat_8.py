from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from job.graph.SubGraph_7.Reformat_8 import *
from job.config.ConfigStore import *


class Reformat_8Test(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/job/graph/SubGraph_7/Reformat_8/in0/schema.json',
            'test/resources/data/job/graph/SubGraph_7/Reformat_8/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/job/graph/SubGraph_7/Reformat_8/out/schema.json',
            'test/resources/data/job/graph/SubGraph_7/Reformat_8/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = Reformat_8(self.spark, dfIn0)
        assertDFEquals(dfOut.select("c_int_new"), dfOutComputed.select("c_int_new"), self.maxUnequalRowsToShow)

    def test_unit_test_1(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/job/graph/SubGraph_7/Reformat_8/in0/schema.json',
            'test/resources/data/job/graph/SubGraph_7/Reformat_8/in0/data/test_unit_test_1.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/job/graph/SubGraph_7/Reformat_8/out/schema.json',
            'test/resources/data/job/graph/SubGraph_7/Reformat_8/out/data/test_unit_test_1.json',
            'out'
        )
        dfOutComputed = Reformat_8(self.spark, dfIn0)
        assertDFEquals(dfOut.select("c_int_new"), dfOutComputed.select("c_int_new"), self.maxUnequalRowsToShow)

    def test_unit_test_2(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/job/graph/SubGraph_7/Reformat_8/in0/schema.json',
            'test/resources/data/job/graph/SubGraph_7/Reformat_8/in0/data/test_unit_test_2.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/job/graph/SubGraph_7/Reformat_8/out/schema.json',
            'test/resources/data/job/graph/SubGraph_7/Reformat_8/out/data/test_unit_test_2.json',
            'out'
        )
        dfOutComputed = Reformat_8(self.spark, dfIn0)
        assertDFEquals(dfOut.select("c_int_new"), dfOutComputed.select("c_int_new"), self.maxUnequalRowsToShow)

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        Utils.initializeFromArgs(
            self.spark,
            Namespace(
              file = f"configs/resources/config/{fabricName}.json",
              config = None,
              overrideJson = None,
              defaultConfFile = None
            )
        )
