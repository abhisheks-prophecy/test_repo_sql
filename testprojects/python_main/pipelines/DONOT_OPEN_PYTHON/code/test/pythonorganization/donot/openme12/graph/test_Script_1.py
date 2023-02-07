from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from pythonorganization.donot.openme12.graph.Script_1 import *
import pythonorganization.donot.openme12.config.ConfigStore as ConfigStore


class Script_1Test(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonorganization/donot/openme12/graph/Script_1/in0/schema.json',
            'test/resources/data/pythonorganization/donot/openme12/graph/Script_1/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonorganization/donot/openme12/graph/Script_1/out0/schema.json',
            'test/resources/data/pythonorganization/donot/openme12/graph/Script_1/out0/data/test_unit_test_0.json',
            'out0'
        )
        dfOut0Computed = Script_1(self.spark, dfIn0)
        assertDFEquals(
            dfOut0.select(
              "c___-- string",
              "c  date",
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal renamed",
              "c  float"
            ),
            dfOut0Computed.select(
              "c___-- string",
              "c  date",
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal renamed",
              "c  float"
            ),
            self.maxUnequalRowsToShow
        )

    def test_unit_test_1(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonorganization/donot/openme12/graph/Script_1/in0/schema.json',
            'test/resources/data/pythonorganization/donot/openme12/graph/Script_1/in0/data/test_unit_test_1.json',
            'in0'
        )
        dfOut0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonorganization/donot/openme12/graph/Script_1/out0/schema.json',
            'test/resources/data/pythonorganization/donot/openme12/graph/Script_1/out0/data/test_unit_test_1.json',
            'out0'
        )
        dfOut0Computed = Script_1(self.spark, dfIn0)
        assertDFEquals(
            dfOut0.select(
              "c___-- string",
              "c  date",
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal renamed",
              "c  float"
            ),
            dfOut0Computed.select(
              "c___-- string",
              "c  date",
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal renamed",
              "c  float"
            ),
            self.maxUnequalRowsToShow
        )

    def test_unit_test_2(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonorganization/donot/openme12/graph/Script_1/in0/schema.json',
            'test/resources/data/pythonorganization/donot/openme12/graph/Script_1/in0/data/test_unit_test_2.json',
            'in0'
        )
        dfOut0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonorganization/donot/openme12/graph/Script_1/out0/schema.json',
            'test/resources/data/pythonorganization/donot/openme12/graph/Script_1/out0/data/test_unit_test_2.json',
            'out0'
        )
        dfOut0Computed = Script_1(self.spark, dfIn0)
        assertDFEquals(
            dfOut0.select(
              "c___-- string",
              "c  date",
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal renamed",
              "c  float"
            ),
            dfOut0Computed.select(
              "c___-- string",
              "c  date",
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal renamed",
              "c  float"
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
