from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from job.graph.Reformat_7 import *
import job.config.ConfigStore as ConfigStore


class Reformat_7Test(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/job/graph/Reformat_7/in0/schema.json',
            'test/resources/data/job/graph/Reformat_7/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/job/graph/Reformat_7/out/schema.json',
            'test/resources/data/job/graph/Reformat_7/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = Reformat_7(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("c_short1", "c_int1", "c_short2", "c_int2"),
            dfOutComputed.select("c_short1", "c_int1", "c_short2", "c_int2"),
            self.maxUnequalRowsToShow
        )

    def test_unit_test_1(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/job/graph/Reformat_7/in0/schema.json',
            'test/resources/data/job/graph/Reformat_7/in0/data/test_unit_test_1.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/job/graph/Reformat_7/out/schema.json',
            'test/resources/data/job/graph/Reformat_7/out/data/test_unit_test_1.json',
            'out'
        )
        dfOutComputed = Reformat_7(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("c_short1", "c_int1", "c_short2", "c_int2"),
            dfOutComputed.select("c_short1", "c_int1", "c_short2", "c_int2"),
            self.maxUnequalRowsToShow
        )

    def test_unit_test_2(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/job/graph/Reformat_7/in0/schema.json',
            'test/resources/data/job/graph/Reformat_7/in0/data/test_unit_test_2.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/job/graph/Reformat_7/out/schema.json',
            'test/resources/data/job/graph/Reformat_7/out/data/test_unit_test_2.json',
            'out'
        )
        dfOutComputed = Reformat_7(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("c_short1", "c_int1", "c_short2", "c_int2"),
            dfOutComputed.select("c_short1", "c_int1", "c_short2", "c_int2"),
            self.maxUnequalRowsToShow
        )

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        ConfigStore.Utils.initializeFromArgs(
            self.spark,
            Namespace(file = f"configs/resources/config/{fabricName}.json", config = None, overrideJson = None)
        )
