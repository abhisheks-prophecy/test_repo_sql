from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from com.main1.pythondepmanagement_1.graph.SchemaTransform_1 import *
import com.main1.pythondepmanagement_1.config.ConfigStore as ConfigStore


class SchemaTransform_1Test(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/SchemaTransform_1/in0/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/SchemaTransform_1/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/SchemaTransform_1/out/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/SchemaTransform_1/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = SchemaTransform_1(self.spark, dfIn0)
        assertDFEquals(dfOut.select("c   short  --"), dfOutComputed.select("c   short  --"), self.maxUnequalRowsToShow)

    def test_unit_test_1(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/SchemaTransform_1/in0/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/SchemaTransform_1/in0/data/test_unit_test_1.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/SchemaTransform_1/out/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/SchemaTransform_1/out/data/test_unit_test_1.json',
            'out'
        )
        dfOutComputed = SchemaTransform_1(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal renamed",
              "c  float",
              "c--boolean",
              "c___-- string",
              "c  date",
              "c_timestamp",
              "squared_short",
              "factorial_short",
              "random_string_value",
              "config_values",
              "concat short int"
            ),
            dfOutComputed.select(
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal renamed",
              "c  float",
              "c--boolean",
              "c___-- string",
              "c  date",
              "c_timestamp",
              "squared_short",
              "factorial_short",
              "random_string_value",
              "config_values",
              "concat short int"
            ),
            self.maxUnequalRowsToShow
        )

    def test_unit_test_2(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/SchemaTransform_1/in0/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/SchemaTransform_1/in0/data/test_unit_test_2.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/SchemaTransform_1/out/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/SchemaTransform_1/out/data/test_unit_test_2.json',
            'out'
        )
        dfOutComputed = SchemaTransform_1(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal renamed",
              "c  float",
              "c--boolean",
              "c___-- string",
              "c  date",
              "c_timestamp",
              "squared_short",
              "factorial_short",
              "random_string_value",
              "config_values",
              "concat short int"
            ),
            dfOutComputed.select(
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal renamed",
              "c  float",
              "c--boolean",
              "c___-- string",
              "c  date",
              "c_timestamp",
              "squared_short",
              "factorial_short",
              "random_string_value",
              "config_values",
              "concat short int"
            ),
            self.maxUnequalRowsToShow
        )

    def test_unit_test_3(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/SchemaTransform_1/in0/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/SchemaTransform_1/in0/data/test_unit_test_3.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/SchemaTransform_1/out/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/SchemaTransform_1/out/data/test_unit_test_3.json',
            'out'
        )
        dfOutComputed = SchemaTransform_1(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal renamed",
              "c  float",
              "c--boolean",
              "c___-- string",
              "c  date",
              "c_timestamp",
              "squared_short",
              "factorial_short",
              "random_string_value",
              "config_values",
              "concat short int"
            ),
            dfOutComputed.select(
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal renamed",
              "c  float",
              "c--boolean",
              "c___-- string",
              "c  date",
              "c_timestamp",
              "squared_short",
              "factorial_short",
              "random_string_value",
              "config_values",
              "concat short int"
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
        dfmain1_pythondepmanagement_1_graph_Lookup_1 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Lookup_1/schema.json',
            'test/resources/data/com/main1/pythondepmanagement_1/graph/Lookup_1/data.json',
            "in0"
        )
        from com.main1.pythondepmanagement_1.graph.Lookup_1 import Lookup_1
        Lookup_1(self.spark, dfmain1_pythondepmanagement_1_graph_Lookup_1)
