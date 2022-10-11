from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, c_st_expr: str=None, c_expr: str=None):
        self.spark = None
        self.update(c_st_expr, c_expr)

    def update(self, c_st_expr: str="concat(industry_code_ANZSIC, industry_name_ANZSIC)", c_expr: str="%11%"):
        self.c_st_expr = c_st_expr
        self.c_expr = c_expr
        pass
