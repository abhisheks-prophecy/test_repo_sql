from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from com.main1.pythondepmanagement_1.graph.Join_5 import *
import com.main1.pythondepmanagement_1.config.ConfigStore as ConfigStore


class Join_5Test(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Join_5/in0/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Join_5/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfIn1 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Join_5/in1/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Join_5/in1/data/test_unit_test_0.json',
            'in1'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Join_5/out/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Join_5/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = Join_5(self.spark, dfIn0, dfIn1)
        assertDFEquals(
            dfOut.select("c_short", "c_int", "c- short", "c_int1"),
            dfOutComputed.select("c_short", "c_int", "c- short", "c_int1"),
            self.maxUnequalRowsToShow
        )

    def test_unit_test_1(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Join_5/in0/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Join_5/in0/data/test_unit_test_1.json',
            'in0'
        )
        dfIn1 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Join_5/in1/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Join_5/in1/data/test_unit_test_1.json',
            'in1'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Join_5/out/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Join_5/out/data/test_unit_test_1.json',
            'out'
        )
        dfOutComputed = Join_5(self.spark, dfIn0, dfIn1)
        assertDFEquals(
            dfOut.select("c_short", "c_int", "c- short", "c_int1"),
            dfOutComputed.select("c_short", "c_int", "c- short", "c_int1"),
            self.maxUnequalRowsToShow
        )

    def test_unit_test_2(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Join_5/in0/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Join_5/in0/data/test_unit_test_2.json',
            'in0'
        )
        dfIn1 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Join_5/in1/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Join_5/in1/data/test_unit_test_2.json',
            'in1'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Join_5/out/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Join_5/out/data/test_unit_test_2.json',
            'out'
        )
        dfOutComputed = Join_5(self.spark, dfIn0, dfIn1)
        assertDFEquals(
            dfOut.select("c_short", "c_int", "c- short", "c_int1"),
            dfOutComputed.select("c_short", "c_int", "c- short", "c_int1"),
            self.maxUnequalRowsToShow
        )

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        ConfigStore.Utils.initializeFromArgs(
            self.spark,
            Namespace(file = f"configs/resources/config/{fabricName}.json", config = None)
        )
