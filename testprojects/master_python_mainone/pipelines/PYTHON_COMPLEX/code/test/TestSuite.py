import unittest

from test.job.graph.test_SetOperation_1 import *
from test.job.graph.test_Join_4 import *
from test.job.graph.test_Limit_1 import *
from test.job.graph.test_Aggregate_1 import *
from test.job.graph.test_Join_3 import *
from test.job.graph.test_Reformat_4 import *
from test.job.graph.test_Script_1 import *
from test.job.graph.test_OrderBy_1 import *
from test.job.graph.test_Reformat_16 import *
from test.job.graph.test_Deduplicate_1 import *
from test.job.graph.test_Limit_3 import *
from test.job.graph.SubGraph_7.test_Script_4 import *
from test.job.graph.SubGraph_7.test_Reformat_8 import *
from test.job.graph.test_Limit_2 import *
from test.job.graph.test_Reformat_7 import *
from test.job.graph.test_SchemaTransform_1 import *
from test.job.graph.test_Limit_9 import *
from test.job.graph.test_Limit_6 import *
from test.job.graph.test_ConfigAndUDF import *
from test.job.graph.test_OrderBy_3 import *
from test.job.graph.test_Join_5 import *
from test.job.graph.test_Reformat_15 import *
from test.job.graph.test_Filter_1 import *

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(unittest.TestSuite())
