from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_all_type_part_parquet = all_type_part_parquet(spark)
    df_all_type_part_parquet = collectMetrics(
        spark, 
        df_all_type_part_parquet, 
        "graph", 
        "4h71IOPBDFyzOnxjjfKPl$$8MhjH0PVU12ED2fJyDLpS", 
        "K1XpTR7-jgHPuX6PHVdPr$$wHEmbxozybFo-eRVP11a3"
    )
    df_RowDistributor_1_out0, df_RowDistributor_1_out1, df_RowDistributor_1_out2, df_RowDistributor_1_out3 = RowDistributor_1(
        spark, 
        df_all_type_part_parquet
    )
    df_RowDistributor_1_out0 = collectMetrics(
        spark, 
        df_RowDistributor_1_out0, 
        "graph", 
        "1SN-pEh_8DmEvfS9UhGNE$$64hDBTC35HIwIiZktE_Ld", 
        "out0"
    )
    df_RowDistributor_1_out1 = collectMetrics(
        spark, 
        df_RowDistributor_1_out1, 
        "graph", 
        "1SN-pEh_8DmEvfS9UhGNE$$64hDBTC35HIwIiZktE_Ld", 
        "W59iy89iTXSgFaxfQEwhW"
    )
    df_RowDistributor_1_out2 = collectMetrics(
        spark, 
        df_RowDistributor_1_out2, 
        "graph", 
        "1SN-pEh_8DmEvfS9UhGNE$$64hDBTC35HIwIiZktE_Ld", 
        "3a-JMPZZW4g-OIK6u8pOH"
    )
    df_RowDistributor_1_out3 = collectMetrics(
        spark, 
        df_RowDistributor_1_out3, 
        "graph", 
        "1SN-pEh_8DmEvfS9UhGNE$$64hDBTC35HIwIiZktE_Ld", 
        "CKp-fYcvwRV-xHIE140JV"
    )
    Lookup_1(spark, df_RowDistributor_1_out1)
    df_csv_special_chars = csv_special_chars(spark)
    df_csv_special_chars = collectMetrics(
        spark, 
        df_csv_special_chars, 
        "graph", 
        "zM4C2oFSMY1j4ofziFf9I$$R7DcTNFgSQS8aDiZv_xUz", 
        "ORTrvFWDCkxykwAXnkn1g$$fwpMbWEu626Vg01d_d77R"
    )
    df_Limit_1 = Limit_1(spark, df_csv_special_chars)
    df_Limit_1 = collectMetrics(
        spark, 
        df_Limit_1, 
        "graph", 
        "e3TJEvqqlprpugw-9O3jw$$Gtd5nRADFkVBz75o2qNn5", 
        "VNcdLoshlAMxxP9q3P6_1$$4ddpNdJqSE6w7cPGroGxj"
    )
    df_Filter_1 = Filter_1(spark, df_Limit_1)
    df_Filter_1 = collectMetrics(
        spark, 
        df_Filter_1, 
        "graph", 
        "5_yNvqIFR0X3gtj0acpc5$$8lshGYMNKW7YDdFdn3a3i", 
        "2sVaJyYIOhDWczNzrH865$$EZP0zXynO1ZUGdxYXqVHn"
    )
    df_ConfigAndUDF = ConfigAndUDF(spark, df_Filter_1)
    df_ConfigAndUDF = collectMetrics(
        spark, 
        df_ConfigAndUDF, 
        "graph", 
        "3ZL3zn7QeM6Yhv-O1O3lw$$FDI6ABdUy6fCp3YqgF1Yv", 
        "0YDYZPDtmMHOdXMO3igze$$uzOCbwhK48a5yijnl6X1B"
    )
    df_OrderBy_1 = OrderBy_1(spark, df_ConfigAndUDF)
    df_OrderBy_1 = collectMetrics(
        spark, 
        df_OrderBy_1, 
        "graph", 
        "LuvCHinHtvzgUCBzLr99X$$C0IBWgHAMSE5ovVqtha7Q", 
        "XMZ9PL47KCK49bhwywt-X$$ApvHMBHWabBeZQaHanXfh"
    )
    df_Deduplicate_1 = Deduplicate_1(spark, df_OrderBy_1)
    df_Deduplicate_1 = collectMetrics(
        spark, 
        df_Deduplicate_1, 
        "graph", 
        "UrZUXs4Zb0DFJ2kliyvD0$$sjteqYHfJEoEm2HSzpcjE", 
        "x7W-pjW-6TjOG3oAscMiK$$3-swEthJhrqJivBfqlgl_"
    )
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_Deduplicate_1)
    df_SchemaTransform_1 = collectMetrics(
        spark, 
        df_SchemaTransform_1, 
        "graph", 
        "MewCOxa3iVvHoWv9bNsev$$2LCHEi6uClGEROiiNv5jo", 
        "9322GqVhXmMJikkf6i0hH$$CjC7KUFPAPleRMaw7mmWn"
    )
    df_SetOperation_1 = SetOperation_1(spark, df_SchemaTransform_1, df_SchemaTransform_1)
    df_SetOperation_1 = collectMetrics(
        spark, 
        df_SetOperation_1, 
        "graph", 
        "JFnDL5Prrj2Z6OXTovov1$$ve0XgF2vT-qYRHHPUnJog", 
        "WU1M6vXwP42j5xhraZQ3H$$5gnYkvzcJzyHGXzBxtNsd"
    )
    df_csv_all_type = csv_all_type(spark)
    df_csv_all_type = collectMetrics(
        spark, 
        df_csv_all_type, 
        "graph", 
        "AsFanA7iJDHUy6iFkD494$$T-Rr6gZuGzb20v-9AtP73", 
        "_posjARhGw9jdalJzj-z7$$evC658R8ZUCPVoEw1Zwo0"
    )
    Target_5(spark, df_csv_all_type)
    df_delta = delta(spark)
    df_delta = collectMetrics(
        spark, 
        df_delta, 
        "graph", 
        "zf0XWHwj9SGWzhPSbiXtO$$Rrw8tZ269ts-muTnc1IJ5", 
        "m-fvj-aHPWpu0ZMPSFDzn$$pGK30odKzqypWhcv-uIx0"
    )
    df_Limit_4 = Limit_4(spark, df_delta)
    df_Limit_4 = collectMetrics(
        spark, 
        df_Limit_4, 
        "graph", 
        "e8gYR43R2yAepDV8orPFH$$mqYXtHvju6Wt2CQ248Vf9", 
        "7Jv_nxOWQ3vIMtWiogYvM$$36XNVyAh1tPUEZN2JMuoK"
    )
    df_Join_1 = Join_1(spark, df_all_type_part_parquet, df_all_type_part_parquet)
    df_Join_1 = collectMetrics(
        spark, 
        df_Join_1, 
        "graph", 
        "-3Yowk1Pyqmmp3G1kygcJ$$uoGVrm34Y1O5vdSyFcVcr", 
        "skdKHf3bW8p-K6g7nyxDC$$bKroTYjQqC3HbFRuqzfzi"
    )
    df_Limit_7 = Limit_7(spark, df_Join_1)
    df_Limit_7 = collectMetrics(
        spark, 
        df_Limit_7, 
        "graph", 
        "jx0AdKV0WFX6qD6_Zndbj$$6yE9NSkJz2Ci1dyJzVJmV", 
        "i1Nsh_PK_FcaOd_3McZm6$$aLqbOLClxwrjo0T9rUVwW"
    )
    df_Repartition_1 = Repartition_1(spark, df_Limit_7)
    df_Repartition_1 = collectMetrics(
        spark, 
        df_Repartition_1, 
        "graph", 
        "AVrA22qbSiO5D8bGEnww6$$d4-r8EQ3lHXDKn2sZL5oX", 
        "yrNcCB_hx8IOjKH2Wh8Jx$$-xZBC0SRBrEmyMsO8m70G"
    )
    df_SubGraph_2 = SubGraph_2(spark, df_Repartition_1, df_RowDistributor_1_out0)
    df_orc_src = orc_src(spark)
    df_orc_src = collectMetrics(
        spark, 
        df_orc_src, 
        "graph", 
        "Z6XmsZw2cEPcc5iAkiyMW$$VhcT7sWlspQ2-KoTh8imV", 
        "44_XPIJCPj1I1tCDMEHmh$$EXLiSwqLDSiRnR3w528in"
    )
    df_Deduplicate_2 = Deduplicate_2(spark, df_orc_src)
    df_Deduplicate_2 = collectMetrics(
        spark, 
        df_Deduplicate_2, 
        "graph", 
        "Y1jPqa-nQfmW_Y3WPDwuf$$P9PIjfA_IXQALkDXfOJxr", 
        "olvbl4JfshuFca-YS1IgG$$MwzaZr7NHXWa5wa4BnmUj"
    )
    df_Reformat_5 = Reformat_5(spark, df_Deduplicate_2)
    df_Reformat_5 = collectMetrics(
        spark, 
        df_Reformat_5, 
        "graph", 
        "jrow1zOQ_eYNkU7aiU1tu$$OdmRkRg-5-BU6S_fxWvIS", 
        "KHVcU5Rq5aJT_x6sZwuKL$$9z7HwEmo_IqtzD5yPwjo5"
    )
    df_Aggregate_1 = Aggregate_1(spark, df_SetOperation_1)
    df_Aggregate_1 = collectMetrics(
        spark, 
        df_Aggregate_1, 
        "graph", 
        "UM37Y4sZy0XbEvnMP3z0d$$PJ-qsFp1oBTaLAY-uUmOZ", 
        "eXuydhO1EqpAMfbq8b8LT$$dqKKSFDwQjydqmvIi4Dft"
    )
    df_Reformat_10 = Reformat_10(spark, df_RowDistributor_1_out3)
    df_Reformat_10 = collectMetrics(
        spark, 
        df_Reformat_10, 
        "graph", 
        "gx4j8IO8rKX5Ig30Ny1ZC$$hG51ERYJ6bf4NFjMZ4dFw", 
        "j0Os5dJub3yCt2NF70V_U$$tmbuE79KgcQKVPKbuOefH"
    )
    df_FlattenSchema_1 = FlattenSchema_1(spark, df_all_type_part_parquet)
    df_FlattenSchema_1 = collectMetrics(
        spark, 
        df_FlattenSchema_1, 
        "graph", 
        "u7Qg_PFHrsE41ccBs1Tsf$$c5l1RrY3J9ksfWatNJE0w", 
        "e804OmuP_L9j4LVHIVgJQ$$0IXbzd5_4VuwMtBPqFWn1"
    )
    df_Filter_3 = Filter_3(spark, df_FlattenSchema_1)
    df_Filter_3 = collectMetrics(
        spark, 
        df_Filter_3, 
        "graph", 
        "0QTZWUp8QoRisqas82OxN$$kWDDsTZzXHaCx7TXllIkB", 
        "09Swm9Q2lijZn9-UBmVCy$$ZIp0z6QWaB-Ub7nWX9DJl"
    )
    df_SQLStatement_1_out, df_SQLStatement_1_out1 = SQLStatement_1(spark, df_Filter_3, df_Filter_3)
    df_SQLStatement_1_out = collectMetrics(
        spark, 
        df_SQLStatement_1_out, 
        "graph", 
        "hqoiE3_vTByedPNRcCJ0x$$4Bi3tMhiCu9eOSnoQLgG1", 
        "XGXDDVJFbDUC5kUIFMevX$$bbjwUl8ZErKYfINzl6dwz"
    )
    df_SQLStatement_1_out1 = collectMetrics(
        spark, 
        df_SQLStatement_1_out1, 
        "graph", 
        "hqoiE3_vTByedPNRcCJ0x$$4Bi3tMhiCu9eOSnoQLgG1", 
        "ap38798z6_gi8oCiTUtlY$$qhFMWyooU1oxUuZFhlanW"
    )
    df_all_components_1_out0, df_all_components_1_out1, df_all_components_1_out2 = all_components_1(
        spark, 
        df_all_type_part_parquet, 
        df_all_type_part_parquet, 
        df_all_type_part_parquet
    )
    df_Limit_5 = Limit_5(spark, df_delta)
    df_Limit_5 = collectMetrics(
        spark, 
        df_Limit_5, 
        "graph", 
        "BRL6b1VATi5syNUzo8RH7$$UdOSsNaBqb7PzrNypQy2H", 
        "tnqt3tW00nFE1obAN03sb$$djd_zMdJ_F1Ns9sfsyofk"
    )
    df_SQLStatement_2 = SQLStatement_2(spark, df_Limit_5)
    df_SQLStatement_2 = collectMetrics(
        spark, 
        df_SQLStatement_2, 
        "graph", 
        "tZecMjCrMn9uKXWvnboEO$$-2Z_nI9Hs9uCql-rigpT1", 
        "yakGkcEED-x_XpZVQBlxW$$pTKTB5OCDDPL77h8Wut6V"
    )
    df_Filter_5 = Filter_5(spark, df_SQLStatement_2)
    df_Filter_5 = collectMetrics(
        spark, 
        df_Filter_5, 
        "graph", 
        "WE0--RRezUv67BUd36H3J$$cbEZGwrxRCoyqo5Fj4ESC", 
        "mUc8iQQ7Gvihi3akGmC3e$$hR4weG_7iGnihmbow-D9n"
    )
    df_Filter_5.cache().count()
    df_Filter_5.unpersist()
    df_text = text(spark)
    df_text = collectMetrics(
        spark, 
        df_text, 
        "graph", 
        "QmGu-UelGs__wu0AWreZx$$Dec9-PSzLbbv6Iyos5iku", 
        "gWPrIMMlkrG2UvHqNPj6D$$FruLC9PEQ8DO2qLMBWDKJ"
    )
    df_Reformat_4 = Reformat_4(spark, df_text)
    df_Reformat_4 = collectMetrics(
        spark, 
        df_Reformat_4, 
        "graph", 
        "nLjPP0w7Cl6MSM6RXHIK0$$iS1hmJ0J0ImQO_7vTMOSk", 
        "Z7JdWCGM7uYZheCRUgwvh$$_JKl5kg-4-dO6AgxV7k8-"
    )
    df_Script_1 = Script_1(spark, df_Aggregate_1)
    df_Script_1 = collectMetrics(
        spark, 
        df_Script_1, 
        "graph", 
        "TFjH9dGegxAAq0-7pL845$$EA0NgKHjYint6E1mmks-E", 
        "ZvgsUJ_9eyK0UXoawagVL$$gD5njuMvJZpys_gRx0vcy"
    )
    df_json_in = json_in(spark)
    df_json_in = collectMetrics(
        spark, 
        df_json_in, 
        "graph", 
        "iEAhKfffRKYROLhkq6LCM$$FUrIc9QCdGEO7TuzFhdTJ", 
        "AkoFj0jmMBshTbQIjr103$$OaRGS-kcQrvnXd_bho8ve"
    )
    df_Reformat_16 = Reformat_16(spark, df_json_in)
    df_Reformat_16 = collectMetrics(
        spark, 
        df_Reformat_16, 
        "graph", 
        "HlaWDG5dsklrL9pm8GG8M$$PhFCGBTLGIr5vXoTnP5v6", 
        "NumrPyiY_QXIWM_GXd7Un$$36iPKHFjqehM2767gFFSP"
    )
    df_catalog = catalog(spark)
    df_catalog = collectMetrics(
        spark, 
        df_catalog, 
        "graph", 
        "__BeSCJf1X2jfa8iyY4-T$$07c-JgTp7QeEW439tKvAI", 
        "cY2VcRtlr_Ypjbo2YK6bm$$RAnzMQK3YjV0hNDb-86v_"
    )
    df_Reformat_9 = Reformat_9(spark, df_RowDistributor_1_out2)
    df_Reformat_9 = collectMetrics(
        spark, 
        df_Reformat_9, 
        "graph", 
        "C29lAbt4R5GEzHuRikLcu$$KJhkbaqwor1_W6sTIOM4N", 
        "GsbGTC9lDwPUI4TRCfxNm$$yFCIAy92G56gcU54tsYp_"
    )
    df_Reformat_9.cache().count()
    df_Reformat_9.unpersist()
    df_Reformat_15 = Reformat_15(spark, df_csv_all_type)
    df_Reformat_15 = collectMetrics(
        spark, 
        df_Reformat_15, 
        "graph", 
        "zjl2OkVETv6JHbonHp74V$$V9bfArO_p5n2ED2KwqGeS", 
        "9jXaJTdFUIjZGLj_vc5cy$$lND32pDnRQ4Mh__Dw292O"
    )
    df_OrderBy_4 = OrderBy_4(spark, df_delta)
    df_OrderBy_4 = collectMetrics(
        spark, 
        df_OrderBy_4, 
        "graph", 
        "PMK5IbeHdFKI_kVBXezxv$$SvIv8eXADLBabqXLFsSye", 
        "JPp8waTMEALCxx-RgmBLe$$7-AKbD5Qi60_VRKRZmJ8w"
    )
    df_Limit_6 = Limit_6(spark, df_catalog)
    df_Limit_6 = collectMetrics(
        spark, 
        df_Limit_6, 
        "graph", 
        "wgo-wNGefvFEyq1Gzk3SC$$p5dqrfNyJCSuzl7ZTNirm", 
        "huZBjTNVhwDZu-dJ8vCLG$$de1GXZu9-Ly9Y6Smlp7Bj"
    )
    df_Script_3 = Script_3(
        spark, 
        df_Limit_6, 
        df_Limit_5, 
        df_OrderBy_4, 
        df_Limit_4, 
        df_Reformat_16, 
        df_Reformat_15, 
        df_OrderBy_1
    )
    df_Script_3 = collectMetrics(
        spark, 
        df_Script_3, 
        "graph", 
        "-3VpysfVzMeGJlzqBNlq_$$ZaNTPymddlozSuJherlQv", 
        "8NNUyPXeI68kaysRjOSPK$$wsEyecXcd28exUXVvELKN"
    )
    df_Limit_8 = Limit_8(spark, df_SubGraph_2)
    df_Limit_8 = collectMetrics(
        spark, 
        df_Limit_8, 
        "graph", 
        "7UKyoy2UqgcsQzTizhlnB$$nQ6VfkPkFqv-P65G9p-K2", 
        "2i3OcF3ZKDVsz86or5b4T$$Hvs_91XxZ8bYvNEaY_gM-"
    )
    df_Reformat_6 = Reformat_6(spark, df_Limit_8)
    df_Reformat_6 = collectMetrics(
        spark, 
        df_Reformat_6, 
        "graph", 
        "XVZA4jaSignv4W79047qL$$c4PKPbLcdXPTOnxegUVdE", 
        "OMvFDXYs4rbzeNHPH195J$$GhXfyPI0OdcDGgZ8EKjX3"
    )
    df_Join_3 = Join_3(spark, df_Reformat_6, df_Reformat_5)
    df_Join_3 = collectMetrics(
        spark, 
        df_Join_3, 
        "graph", 
        "9zm0eAeg6q1PJQlsk9axq$$POiAWHO3kRjH1PbozriWM", 
        "jf3PTjvZYdDjCpHgnyHo4$$3K68Ip_CK3ndgPjPUY9OU"
    )
    df_avro = avro(spark)
    df_avro = collectMetrics(
        spark, 
        df_avro, 
        "graph", 
        "5CWHE-tPSkfgaUUbMv5u5$$bz3wkz4t1Qs8S2xkBshkc", 
        "O7MlD4SsSNAngvEmyO2mL$$EPYlbrlI9CP94b-tBwNsN"
    )
    df_OrderBy_3 = OrderBy_3(spark, df_avro)
    df_OrderBy_3 = collectMetrics(
        spark, 
        df_OrderBy_3, 
        "graph", 
        "F-O0ZWyymLHQ7NAiD4HwH$$mRW3uIYVYRgYXJeBJQ7JX", 
        "23_EUJ5mmZmxkUF_FP06-$$05CM-G0C71N7lAUdf7DdA"
    )
    df_Reformat_7 = Reformat_7(spark, df_OrderBy_3)
    df_Reformat_7 = collectMetrics(
        spark, 
        df_Reformat_7, 
        "graph", 
        "oEhaLSE6b3A1LJxg3Bkz_$$l12DOtVNEzu8reItRujM_", 
        "kX6osjcARyB_vtVysaBkg$$k2MRDGFfUnWalJzYqYMPk"
    )
    df_Join_4 = Join_4(spark, df_Join_3, df_Reformat_7)
    df_Join_4 = collectMetrics(
        spark, 
        df_Join_4, 
        "graph", 
        "iaaTx289zYQMmadlDzy-6$$S7WF3Ans7xm2lEL9OLiaJ", 
        "kDG9ErGZEr6_nUT7EIVl3$$8ycvBvknk6dwqA7MYQ3ch"
    )
    df_Script_6 = Script_6(
        spark, 
        df_all_components_1_out0, 
        df_all_components_1_out1, 
        df_all_components_1_out1, 
        df_all_components_1_out1, 
        df_all_components_1_out2, 
        df_all_components_1_out2, 
        df_all_components_1_out2
    )
    df_Script_6 = collectMetrics(
        spark, 
        df_Script_6, 
        "graph", 
        "5kjgFXnDVKXnZbuLD5jdC$$SyZ5rbYl2DTmA2SBi2eLW", 
        "EvF8Dow274Ex0OhnayGYM$$i6UX0uzgLjqHwTvtrZnVu"
    )
    df_Reformat_11 = Reformat_11(spark, df_Script_6)
    df_Reformat_11 = collectMetrics(
        spark, 
        df_Reformat_11, 
        "graph", 
        "AaIDfqAztEkgFn5KRvY02$$XDz5fzxaLT2bgSZSaafW6", 
        "CslDD0qqB0eax70M2UCCZ$$8mL1DtQCpFdTWxsvaY3xk"
    )
    df_Reformat_11.cache().count()
    df_Reformat_11.unpersist()
    df_Limit_3 = Limit_3(spark, df_Script_1)
    df_Limit_3 = collectMetrics(
        spark, 
        df_Limit_3, 
        "graph", 
        "uCjYghp7xENFfccEAwf0i$$J4oUxMWcsQ8z9WTgmEk5E", 
        "jRQ-hDJGF8airJSOEC9CK$$d49_2pfAycCRBy2dk2ro4"
    )
    df_Limit_3.cache().count()
    df_Limit_3.unpersist()
    df_SubGraph_7 = SubGraph_7(spark, df_Script_3)
    df_Limit_2 = Limit_2(spark, df_SubGraph_7)
    df_Limit_2 = collectMetrics(
        spark, 
        df_Limit_2, 
        "graph", 
        "V6-I5pKLScaJ5c3LmfEIq$$tVAu-_1xgolb5UWzL072v", 
        "CoKMTNCib_GoZndahl6ba$$Z1Z2zektg1WpYFnHzT1UB"
    )
    df_Limit_2.cache().count()
    df_Limit_2.unpersist()
    df_Source_1 = Source_1(spark)
    df_Source_1 = collectMetrics(
        spark, 
        df_Source_1, 
        "graph", 
        "m7IaU0XgW06uSIbFGef0B$$8YjTXHRWeIcuhvz_iG2NZ", 
        "gC-8YSjj5-83Mi4jzyWeE$$eWVZk_fAShNrHFWsAWIsA"
    )
    df_Limit_9 = Limit_9(spark, df_Join_4)
    df_Limit_9 = collectMetrics(
        spark, 
        df_Limit_9, 
        "graph", 
        "cKFCDnAFle9Y7_Z75f91D$$-8WecmUvnsMzFmFYraCx4", 
        "0GItvH4CK7tzzNvabLTsh$$r7w5ecu0XsEojWY08vimA"
    )
    df_WindowFunction_1 = WindowFunction_1(spark, df_RowDistributor_1_out3)
    df_WindowFunction_1 = collectMetrics(
        spark, 
        df_WindowFunction_1, 
        "graph", 
        "6M_hg9gAzr8d7tp6EWYAP$$6ze3-Fnz0TJL3FHRhDRoo", 
        "THZf4U0Afr0WwFlHIExkT$$ZcG-fixeZPUAPkqvmsgpR"
    )
    Script_2(spark, df_WindowFunction_1)
    df_ComplexExpr = ComplexExpr(spark, df_Source_1)
    df_ComplexExpr = collectMetrics(
        spark, 
        df_ComplexExpr, 
        "graph", 
        "jLMRpTBR_DSQXcbn3-w5t$$DURs_-zDLoFxoxJ2U3jav", 
        "QeW6AXzdAfKrFZuBLp-aB$$4TUqZXq0hzrXa7yNQJNBJ"
    )
    df_OrderBy_5 = OrderBy_5(spark, df_ComplexExpr)
    df_OrderBy_5 = collectMetrics(
        spark, 
        df_OrderBy_5, 
        "graph", 
        "VItSFIYTxm_DTe_NTG7TA$$zuwJmTgGpxrS-lqeNt_Cw", 
        "Czcyk9sHItA4nU32spgaq$$qKeZEqV15XoqnCgXV2j2K"
    )
    df_Limit_10 = Limit_10(spark, df_OrderBy_5)
    df_Limit_10 = collectMetrics(
        spark, 
        df_Limit_10, 
        "graph", 
        "HM4vsrSP_m_KkRM3ADWAJ$$ijUOLjOiTPRJp_8Bc-THA", 
        "ZC8SywT2cNRc4oLkXKR_e$$NHhRk0hgQU5bh0-_ZZDiB"
    )
    df_Limit_10.cache().count()
    df_Limit_10.unpersist()
    OUT_PYTHON_DEP_MANAGEMENT_ALL(spark, df_Reformat_10)
    df_Join_5 = Join_5(spark, df_Limit_9, df_Reformat_4)
    df_Join_5 = collectMetrics(
        spark, 
        df_Join_5, 
        "graph", 
        "BxLGlSJsIH_GX44ps93vE$$GQFLMIHvyp20PqCdwSHu1", 
        "dvt3l9wKZJwxnYzggRdLH$$qFCG8vzzPdKTat2OjHZWH"
    )
    df_Join_5.cache().count()
    df_Join_5.unpersist()
    df_OrderBy_6 = OrderBy_6(spark, df_SQLStatement_1_out1)
    df_OrderBy_6 = collectMetrics(
        spark, 
        df_OrderBy_6, 
        "graph", 
        "VOY7oyH26XaORCcitpYuF$$uaaSKA-bYxKmVfhufTwq0", 
        "UZTRej9Ztq7iZwLbTGYrW$$Ea-z9WPEqyZegM9gM2Hoj"
    )
    df_OrderBy_6.cache().count()
    df_OrderBy_6.unpersist()
    df_Deduplicate_3 = Deduplicate_3(spark, df_SQLStatement_1_out)
    df_Deduplicate_3 = collectMetrics(
        spark, 
        df_Deduplicate_3, 
        "graph", 
        "14eZ8cuKHhZ0Hl1NEtqwU$$gI1k_R-uxdHedGYX4_YOW", 
        "7s94ovFzSgyfN359xu4WO$$vpY3azCToCGknAOszAeE0"
    )
    df_Deduplicate_3.cache().count()
    df_Deduplicate_3.unpersist()

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
    spark.conf.set("spark_config1", "spark_config1 value")
    spark.conf.set("spark_config2", "spark_config2 value")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/PYTHON_DEP_MANAGEMENT_ALL_Clone")
    spark.sparkContext._jsc.hadoopConfiguration().set("hadoop_config1", "hadoop_config1 value")
    spark.sparkContext._jsc.hadoopConfiguration().set("hadoop_config2", "hadoop_config2 value")
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = spark.conf.get("prophecy.project.id") + "/" + "pipelines/PYTHON_DEP_MANAGEMENT_ALL_Clone"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
