from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(self, c_st_expr: str=None, c_expr: str=None, c_string: str=None, c_int: int=None):
        self.spark = None
        self.update(c_st_expr, c_expr, c_string, c_int)

    def update(
            self,
            c_st_expr: str="concat(industry_code_ANZSIC, industry_name_ANZSIC)", 
            c_expr: str="%11%", 
            c_string: str="this is a test string", 
            c_int: int=22
    ):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.c_st_expr = c_st_expr
        self.c_expr = c_expr
        self.c_string = c_string
        self.c_int = self.get_int_value(c_int)
        pass
