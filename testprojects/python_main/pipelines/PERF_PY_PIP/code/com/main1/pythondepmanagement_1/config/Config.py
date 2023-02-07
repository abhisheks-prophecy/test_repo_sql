from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            JDBC_URL: str=None, 
            JDBC_SOURCE_TABLE: str=None, 
            CONFIG_STR: str=None, 
            CONFIG_BOOLEAN: bool=None, 
            CONFIG_DOUBLE: float=None, 
            CONFIG_INT: int=None, 
            CONFIG_FLOAT: float=None, 
            CONFIG_SHORT: int=None, 
            CONFIG_DB_SECRETS: str=None, 
            EXPR_COMPLEX_DATES: str=None, 
            c_int_11: int=None, 
            c_st_expr: str=None, 
            c_decimal_renamed: str=None, 
            c_repartition_expr: str=None, 
            c_repartition_colname: str=None, 
            c_sql_pattern: str=None, 
            c_row_distributor_expr: str=None, 
            c_join_expr: str=None, 
            c_1: int=None, 
            c_0: int=None, 
            c_row_distributor: str=None, 
            c_order_by_expr: str=None, 
            c_expr_deduplicate: str=None, 
            c_decimal: str=None, 
            c_rowdistributor_complex_expr: str=None, 
            c_aggregate_expr: str=None, 
            c_aggregate_string: str=None, 
            c_aggregate_float_name: str=None, 
            c_row_number: str=None, 
            c_long_wf: str=None, 
            c_wf_orderby_expr: str=None, 
            c_regex1: str=None, 
            c_regex2: str=None, 
            c_str: str=None, 
            c_config_1: str=None, 
            c_config_2: str=None, 
            c_config_3: str=None, 
            c_config_4: str=None, 
            c_config_5: str=None, 
            c_config_6: str=None, 
            c_config_7: str=None, 
            c_config_8: str=None, 
            c_config_9: str=None, 
            c_config_10: str=None, 
            c_config_11: str=None, 
            c_config_12: str=None, 
            c_config_13: str=None, 
            c_config_14: str=None, 
            c_config_15: str=None, 
            c_config_16: str=None, 
            c_config_17: str=None, 
            c_config_18: str=None, 
            c_config_19: str=None, 
            c_config_20: str=None, 
            c_config_21: str=None, 
            c_config_22: str=None, 
            c_config_23: str=None, 
            c_config_24: str=None, 
            c_config_25: str=None, 
            c_config_26: str=None, 
            c_config_27: str=None, 
            c_config_28: str=None, 
            c_config_29: str=None, 
            c_config_30: str=None, 
            c_config_31: str=None, 
            c_config_32: str=None, 
            c_config_33: str=None, 
            c_config_34: str=None, 
            c_config_35: str=None, 
            c_config_36: str=None, 
            c_config_37: str=None, 
            c_config_38: str=None, 
            c_config_39: str=None, 
            c_config_40: str=None, 
            c_config_41: str=None, 
            c_config_42: str=None, 
            c_config_43: str=None, 
            c_config_44: str=None, 
            c_config_45: str=None, 
            c_config_46: str=None, 
            c_config_47: str=None, 
            c_config_48: str=None, 
            c_config_49: str=None, 
            c_config_50: str=None, 
            AI_MIN_DATETIME: str=None
    ):
        self.spark = None
        self.update(
            JDBC_URL, 
            JDBC_SOURCE_TABLE, 
            CONFIG_STR, 
            CONFIG_BOOLEAN, 
            CONFIG_DOUBLE, 
            CONFIG_INT, 
            CONFIG_FLOAT, 
            CONFIG_SHORT, 
            CONFIG_DB_SECRETS, 
            EXPR_COMPLEX_DATES, 
            c_int_11, 
            c_st_expr, 
            c_decimal_renamed, 
            c_repartition_expr, 
            c_repartition_colname, 
            c_sql_pattern, 
            c_row_distributor_expr, 
            c_join_expr, 
            c_1, 
            c_0, 
            c_row_distributor, 
            c_order_by_expr, 
            c_expr_deduplicate, 
            c_decimal, 
            c_rowdistributor_complex_expr, 
            c_aggregate_expr, 
            c_aggregate_string, 
            c_aggregate_float_name, 
            c_row_number, 
            c_long_wf, 
            c_wf_orderby_expr, 
            c_regex1, 
            c_regex2, 
            c_str, 
            c_config_1, 
            c_config_2, 
            c_config_3, 
            c_config_4, 
            c_config_5, 
            c_config_6, 
            c_config_7, 
            c_config_8, 
            c_config_9, 
            c_config_10, 
            c_config_11, 
            c_config_12, 
            c_config_13, 
            c_config_14, 
            c_config_15, 
            c_config_16, 
            c_config_17, 
            c_config_18, 
            c_config_19, 
            c_config_20, 
            c_config_21, 
            c_config_22, 
            c_config_23, 
            c_config_24, 
            c_config_25, 
            c_config_26, 
            c_config_27, 
            c_config_28, 
            c_config_29, 
            c_config_30, 
            c_config_31, 
            c_config_32, 
            c_config_33, 
            c_config_34, 
            c_config_35, 
            c_config_36, 
            c_config_37, 
            c_config_38, 
            c_config_39, 
            c_config_40, 
            c_config_41, 
            c_config_42, 
            c_config_43, 
            c_config_44, 
            c_config_45, 
            c_config_46, 
            c_config_47, 
            c_config_48, 
            c_config_49, 
            c_config_50, 
            AI_MIN_DATETIME
        )

    def update(
            self,
            JDBC_URL: str="jdbc:mysql://18.144.156.219:3306/test_database", 
            JDBC_SOURCE_TABLE: str="test_table", 
            CONFIG_STR: str=None, 
            CONFIG_BOOLEAN: bool=True, 
            CONFIG_DOUBLE: float=1.00123211232E7, 
            CONFIG_INT: int=None, 
            CONFIG_FLOAT: float=4567546.5, 
            CONFIG_SHORT: int=120, 
            CONFIG_DB_SECRETS: str="qasecrets:mysql_user", 
            EXPR_COMPLEX_DATES: str="(((((date_add(date_trunc(date_format(date_sub(date_add(current_date(), 2), 2), 'yyyy MMM dd'), 'YEAR'), 1) < date_sub(current_timestamp(), 2)) OR (date_add(from_unixtime(0, 'yyyy-MM-dd HH:mm:ss'), 2) < date_add(from_utc_timestamp(c_timestamp, 'Asia/Seoul'), 2))) OR (next_day('2015-01-14', 'TU') < current_date())) OR ((to_date('2009-07-30 04:17:52') < to_timestamp(c_date)) OR (add_months(c_timestamp, 1) < add_months(c_date, 2)))) AND ((c_int % 2) = 0))", 
            c_int_11: int=10, 
            c_st_expr: str="concat(`c   short  --`, `c-int-column type`)", 
            c_decimal_renamed: str="`c-decimal renamed`", 
            c_repartition_expr: str="concat(`c  float`, `c--boolean`)", 
            c_repartition_colname: str="`c_float-__  `", 
            c_sql_pattern: str="%[^aeiou]@%", 
            c_row_distributor_expr: str="(((col(\"`c_struct -- _  `.`c_double - of a struct _`\") > lit(20)) | (col(\"`c_date-for today`\") == lit(\"2005-04-16\"))) & col(\"`c_array-string  _ string`\")[1].like(\"%7%\"))", 
            c_join_expr: str="(in0.`- c long` = in1.`- c long`)", 
            c_1: int=1, 
            c_0: int=0, 
            c_row_distributor: str="(((`c_struct -- _  `.`c_double - of a struct _` > 20) OR (`c_date-for today` = '2005-04-16')) AND `c_array-string  _ string`[1] LIKE '%7%')", 
            c_order_by_expr: str="concat(c_int, c_long)", 
            c_expr_deduplicate: str="concat(`c  float`, `c   short  --`)", 
            c_decimal: str="`c-decimal`", 
            c_rowdistributor_complex_expr: str="((p_string LIKE '%a%' OR RLIKE(p_string, '%A%')) OR ((`c_decimal  -  ` = 12321) AND `c_array-string  _ string`[0] LIKE '%3%'))", 
            c_aggregate_expr: str="first(`c   short  --`)", 
            c_aggregate_string: str="`c___-- string`", 
            c_aggregate_float_name: str="`c float`", 
            c_row_number: str="row_number()", 
            c_long_wf: str="`- c long`", 
            c_wf_orderby_expr: str="concat(`c -  boolean _  `, c_double)", 
            c_regex1: str="^[_A-Za-z0-9-]+(\\\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\\\.[A-Za-z0-9]+)*(\\\\.[A-Za-z]{2,})", 
            c_regex2: str="((?=.*)(?=.*[a-z])(?=.*[A-Z])(?=.*[@#%]).{6,20})", 
            c_str: str="stringwith$$one#%^&*()-=!@#", 
            c_config_1: str="this is a test!@#^&*()_=-", 
            c_config_2: str="this is a test!@#^&*()_=-", 
            c_config_3: str="this is a test!@#^&*()_=-", 
            c_config_4: str="this is a test!@#^&*()_=-", 
            c_config_5: str="this is a test!@#^&*()_=-", 
            c_config_6: str="this is a test!@#^&*()_=-", 
            c_config_7: str="this is a test!@#^&*()_=-", 
            c_config_8: str="this is a test!@#^&*()_=-", 
            c_config_9: str="this is a test!@#^&*()_=-", 
            c_config_10: str="this is a test!@#^&*()_=-", 
            c_config_11: str="this is a test!@#^&*()_=- asd", 
            c_config_12: str="this is a test!@#^&*()_=- asd", 
            c_config_13: str="this is a test!@#^&*()_=- asd", 
            c_config_14: str="this is a test!@#^&*()_=- asd", 
            c_config_15: str="this is a test!@#^&*()_=- asd", 
            c_config_16: str="this is a test!@#^&*()_=- asd", 
            c_config_17: str="this is a test!@#^&*()_=- asd", 
            c_config_18: str="this is a test!@#^&*()_=- asd", 
            c_config_19: str="this is a test!@#^&*()_=- asd", 
            c_config_20: str="this is a test!@#^&*()_=- asd", 
            c_config_21: str="this is a test!@#^&*()_=- asd", 
            c_config_22: str="this is a test!@#^&*()_=- asd", 
            c_config_23: str="this is a test!@#^&*()_=- asd", 
            c_config_24: str="this is a test!@#^&*()_=- asd", 
            c_config_25: str="this is a test!@#^&*()_=- asd", 
            c_config_26: str="this is a test!@#^&*()_=- asd", 
            c_config_27: str="this is a test!@#^&*()_=- asd", 
            c_config_28: str="this is a test!@#^&*()_=- asd", 
            c_config_29: str="this is a test!@#^&*()_=- asd", 
            c_config_30: str="this is a test!@#^&*()_=- asd", 
            c_config_31: str="this is a test!@#^&*()_=- asd", 
            c_config_32: str="this is a test!@#^&*()_=- asd", 
            c_config_33: str="this is a test!@#^&*()_=- asd", 
            c_config_34: str="this is a test!@#^&*()_=- asd", 
            c_config_35: str="this is a test!@#^&*()_=- asd", 
            c_config_36: str="this is a test!@#^&*()_=- asdasd", 
            c_config_37: str="this is a test!@#^&*()_=- asdasd", 
            c_config_38: str="this is a test!@#^&*()_=- asdasd", 
            c_config_39: str="this is a test!@#^&*()_=- asdasd", 
            c_config_40: str="this is a test!@#^&*()_=- asdasd", 
            c_config_41: str="this is a test!@#^&*()_=- asdasd", 
            c_config_42: str="this is a test!@#^&*()_=- asdasd", 
            c_config_43: str="this is a test!@#^&*()_=- asdasd", 
            c_config_44: str="this is a test!@#^&*()_=- asdasd", 
            c_config_45: str="this is a test!@#^&*()_=- asdasd", 
            c_config_46: str="this is a test!@#^&*()_=- asdasd", 
            c_config_47: str="this is a test!@#^&*()_=- asdasd", 
            c_config_48: str="this is a test!@#^&*()_=- asdasd", 
            c_config_49: str="this is a test!@#^&*()_=- asdasd", 
            c_config_50: str="this is a test!@#^&*()_=- asdasd", 
            AI_MIN_DATETIME: str="2020-01-02 11:11:11"
    ):
        self.JDBC_URL = JDBC_URL
        self.JDBC_SOURCE_TABLE = JDBC_SOURCE_TABLE
        self.CONFIG_STR = CONFIG_STR
        self.CONFIG_BOOLEAN = self.get_bool_value(CONFIG_BOOLEAN)
        self.CONFIG_DOUBLE = self.get_float_value(CONFIG_DOUBLE)
        self.CONFIG_INT = self.get_int_value(CONFIG_INT)
        self.CONFIG_FLOAT = self.get_float_value(CONFIG_FLOAT)
        self.CONFIG_SHORT = self.get_int_value(CONFIG_SHORT)

        if CONFIG_DB_SECRETS is not None:
            self.CONFIG_DB_SECRETS = self.get_dbutils(self.spark).secrets.get(*CONFIG_DB_SECRETS.split(":"))

        self.EXPR_COMPLEX_DATES = EXPR_COMPLEX_DATES
        self.c_int_11 = self.get_int_value(c_int_11)
        self.c_st_expr = c_st_expr
        self.c_decimal_renamed = c_decimal_renamed
        self.c_repartition_expr = c_repartition_expr
        self.c_repartition_colname = c_repartition_colname
        self.c_sql_pattern = c_sql_pattern
        self.c_row_distributor_expr = c_row_distributor_expr
        self.c_join_expr = c_join_expr
        self.c_1 = self.get_int_value(c_1)
        self.c_0 = self.get_int_value(c_0)
        self.c_row_distributor = c_row_distributor
        self.c_order_by_expr = c_order_by_expr
        self.c_expr_deduplicate = c_expr_deduplicate
        self.c_decimal = c_decimal
        self.c_rowdistributor_complex_expr = c_rowdistributor_complex_expr
        self.c_aggregate_expr = c_aggregate_expr
        self.c_aggregate_string = c_aggregate_string
        self.c_aggregate_float_name = c_aggregate_float_name
        self.c_row_number = c_row_number
        self.c_long_wf = c_long_wf
        self.c_wf_orderby_expr = c_wf_orderby_expr
        self.c_regex1 = c_regex1
        self.c_regex2 = c_regex2
        self.c_str = c_str
        self.c_config_1 = c_config_1
        self.c_config_2 = c_config_2
        self.c_config_3 = c_config_3
        self.c_config_4 = c_config_4
        self.c_config_5 = c_config_5
        self.c_config_6 = c_config_6
        self.c_config_7 = c_config_7
        self.c_config_8 = c_config_8
        self.c_config_9 = c_config_9
        self.c_config_10 = c_config_10
        self.c_config_11 = c_config_11
        self.c_config_12 = c_config_12
        self.c_config_13 = c_config_13
        self.c_config_14 = c_config_14
        self.c_config_15 = c_config_15
        self.c_config_16 = c_config_16
        self.c_config_17 = c_config_17
        self.c_config_18 = c_config_18
        self.c_config_19 = c_config_19
        self.c_config_20 = c_config_20
        self.c_config_21 = c_config_21
        self.c_config_22 = c_config_22
        self.c_config_23 = c_config_23
        self.c_config_24 = c_config_24
        self.c_config_25 = c_config_25
        self.c_config_26 = c_config_26
        self.c_config_27 = c_config_27
        self.c_config_28 = c_config_28
        self.c_config_29 = c_config_29
        self.c_config_30 = c_config_30
        self.c_config_31 = c_config_31
        self.c_config_32 = c_config_32
        self.c_config_33 = c_config_33
        self.c_config_34 = c_config_34
        self.c_config_35 = c_config_35
        self.c_config_36 = c_config_36
        self.c_config_37 = c_config_37
        self.c_config_38 = c_config_38
        self.c_config_39 = c_config_39
        self.c_config_40 = c_config_40
        self.c_config_41 = c_config_41
        self.c_config_42 = c_config_42
        self.c_config_43 = c_config_43
        self.c_config_44 = c_config_44
        self.c_config_45 = c_config_45
        self.c_config_46 = c_config_46
        self.c_config_47 = c_config_47
        self.c_config_48 = c_config_48
        self.c_config_49 = c_config_49
        self.c_config_50 = c_config_50
        self.AI_MIN_DATETIME = AI_MIN_DATETIME
        pass
