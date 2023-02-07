from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from pythonbasic.test.mainone.graph.Reformat_4 import *
import pythonbasic.test.mainone.config.ConfigStore as ConfigStore


class Reformat_4Test(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_4/in0/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_4/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_4/out/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_4/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = Reformat_4(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "c- short",
              "c  - int",
              "- c long",
              "c_decimal  -  ",
              "c_float-__  ",
              "c -  boolean _  ",
              "c_double",
              "c-string",
              "c_date-for today",
              "c_timestamp  __ for--today",
              "c-bytes",
              "c-binary",
              "p_date"
            ),
            dfOutComputed.select(
              "c- short",
              "c  - int",
              "- c long",
              "c_decimal  -  ",
              "c_float-__  ",
              "c -  boolean _  ",
              "c_double",
              "c-string",
              "c_date-for today",
              "c_timestamp  __ for--today",
              "c-bytes",
              "c-binary",
              "p_date"
            ),
            self.maxUnequalRowsToShow
        )

    def test_unit_test_1(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_4/in0/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_4/in0/data/test_unit_test_1.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_4/out/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_4/out/data/test_unit_test_1.json',
            'out'
        )
        dfOutComputed = Reformat_4(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "c- short",
              "c  - int",
              "- c long",
              "c_decimal  -  ",
              "c_float-__  ",
              "c -  boolean _  ",
              "c_double",
              "c-string",
              "c_date-for today",
              "c_timestamp  __ for--today",
              "c-bytes",
              "c-binary",
              "p_date"
            ),
            dfOutComputed.select(
              "c- short",
              "c  - int",
              "- c long",
              "c_decimal  -  ",
              "c_float-__  ",
              "c -  boolean _  ",
              "c_double",
              "c-string",
              "c_date-for today",
              "c_timestamp  __ for--today",
              "c-bytes",
              "c-binary",
              "p_date"
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
