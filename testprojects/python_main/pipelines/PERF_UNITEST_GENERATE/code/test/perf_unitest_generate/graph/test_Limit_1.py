from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from perf_unitest_generate.graph.Limit_1 import *
import perf_unitest_generate.config.ConfigStore as ConfigStore


class Limit_1Test(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/perf_unitest_generate/graph/Limit_1/in0/schema.json',
            'test/resources/data/perf_unitest_generate/graph/Limit_1/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/perf_unitest_generate/graph/Limit_1/out/schema.json',
            'test/resources/data/perf_unitest_generate/graph/Limit_1/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = Limit_1(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "account_flags",
              "account_open_date",
              "country_code",
              "customer_id",
              "email",
              "first_name",
              "last_name",
              "phone"
            ),
            dfOutComputed.select(
              "account_flags",
              "account_open_date",
              "country_code",
              "customer_id",
              "email",
              "first_name",
              "last_name",
              "phone"
            ),
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
        dfgraph_Lookup_1 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/perf_unitest_generate/graph/Lookup_1/schema.json',
            'test/resources/data/perf_unitest_generate/graph/Lookup_1/data.json',
            "in0"
        )
        from perf_unitest_generate.graph.Lookup_1 import Lookup_1
        Lookup_1(self.spark, dfgraph_Lookup_1)
