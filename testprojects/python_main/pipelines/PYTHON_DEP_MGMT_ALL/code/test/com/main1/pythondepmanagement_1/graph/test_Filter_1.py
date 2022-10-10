from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from com.main1.pythondepmanagement_1.graph.Filter_1 import *
import com.main1.pythondepmanagement_1.config.ConfigStore as ConfigStore


class Filter_1Test(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Filter_1/in0/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Filter_1/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Filter_1/out/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Filter_1/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = Filter_1(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal",
              "c  float",
              "c--boolean",
              "c- - -double",
              "c___-- string",
              "c  date",
              "c_timestamp"
            ),
            dfOutComputed.select(
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal",
              "c  float",
              "c--boolean",
              "c- - -double",
              "c___-- string",
              "c  date",
              "c_timestamp"
            ),
            self.maxUnequalRowsToShow
        )

    def test_unit_test_1(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Filter_1/in0/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Filter_1/in0/data/test_unit_test_1.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Filter_1/out/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Filter_1/out/data/test_unit_test_1.json',
            'out'
        )
        dfOutComputed = Filter_1(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal",
              "c  float",
              "c--boolean",
              "c- - -double",
              "c___-- string",
              "c  date",
              "c_timestamp"
            ),
            dfOutComputed.select(
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal",
              "c  float",
              "c--boolean",
              "c- - -double",
              "c___-- string",
              "c  date",
              "c_timestamp"
            ),
            self.maxUnequalRowsToShow
        )

    def test_unit_test_2(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Filter_1/in0/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Filter_1/in0/data/test_unit_test_2.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Filter_1/out/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Filter_1/out/data/test_unit_test_2.json',
            'out'
        )
        dfOutComputed = Filter_1(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal",
              "c  float",
              "c--boolean",
              "c- - -double",
              "c___-- string",
              "c  date",
              "c_timestamp"
            ),
            dfOutComputed.select(
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal",
              "c  float",
              "c--boolean",
              "c- - -double",
              "c___-- string",
              "c  date",
              "c_timestamp"
            ),
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
