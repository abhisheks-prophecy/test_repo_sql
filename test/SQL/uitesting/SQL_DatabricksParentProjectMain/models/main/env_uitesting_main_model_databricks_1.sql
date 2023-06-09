{% set v_dict_model_db = { 'a' : 1, 'b' : 2 } %}
{% set v_list_model_db = [1, 2, 3, 4, 5] %}
{% set v_int_model_db = 10 %}
{% set v_float_model_db = -10.12 %}
{% set v_boolean_model_db = True %}





WITH all_type_non_partitioned AS (

  SELECT * 
  
  FROM {{ source('aliasbase_spark_catalog_qa_db_warehouse', 'all_type_non_partitioned') }}

),

all_type_parquet AS (

  SELECT * 
  
  FROM {{ source('aliasbase_spark_catalog_qa_database', 'all_type_parquet') }}

),

Reformat_1 AS (

  SELECT 
    c_tinyint AS c_tinyint,
    c_smallint AS c_smallint,
    c_int AS c_int,
    c_bigint AS c_bigint,
    c_float AS c_float,
    c_double AS c_double,
    c_string AS c_string,
    c_boolean AS c_boolean,
    c_array AS c_array,
    c_struct AS c_struct,
    {{ SQL_DatabricksParentProjectMain.qa_boolean_macro('c_string') }} AS c_maco_1,
    concat({{ SQL_DatabricksParentProjectMain.qa_concat_macro_column('c_string') }}, {{ SQL_BaseDependencyProjectAllGit.qa_concat_macro_base("test random string") }}) AS c_maco_2,
    {% for c_i in range(0, 5) %}
      concat(c_string, {{c_i}}) AS cfor_{{c_i}},
    {% endfor %}
    
    {% if v_int_model_db > 10 or                                       var('v_dict_project_level')['a'] == 10 or                    v_list_model_db[0] == 1 %}
      concat(c_string, {{ SQL_DatabricksParentProjectMain.qa_concat_macro('c_string') }}) AS c_if,
    {% else %}
      concat(c_string, c_double) AS c_if,
    {% endif %}
    {{ SQL_DatabricksParentProjectMain.databricks__language_specific_concat() }} AS c_macro_3,
    {{ SQL_DatabricksParentProjectMain.qa_macro_call_another_macro_column('c_string') }} AS c_macro_4,
    {{ SQL_BaseDependencyProjectAllGit._round_function_base(10.12321, 2) }} AS c_dep_project_macro
  
  FROM all_type_parquet AS in0

),

Join_1 AS (

  SELECT 
    in1.c_tinyint AS c_tinyint,
    in1.c_smallint AS c_smallint,
    in1.c_int AS c_int,
    in1.c_bigint AS c_bigint,
    in1.c_float AS c_float,
    in1.c_double AS c_double,
    in1.c_string AS c_string,
    in1.c_boolean AS c_boolean,
    in1.c_array AS c_array,
    in1.c_struct AS c_struct
  
  FROM all_type_non_partitioned AS in0
  INNER JOIN Reformat_1 AS in1
     ON in0.c_tinyint == in1.c_tinyint

)

SELECT *

FROM Join_1
