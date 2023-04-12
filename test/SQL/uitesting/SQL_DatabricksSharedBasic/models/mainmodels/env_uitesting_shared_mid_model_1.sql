

{% set v_int = 10 %}
{% set v_dict = { 'a' : 10, 'b' : 20 } %}


WITH env_uitesting_shared_parent_model_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_shared_parent_model_1')}}

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
    {{ SQL_DatabricksSharedBasic.qa_concat_function_main('c_string', 'c_boolean') }} AS c_macro,
    {% if v_int > 0 or   var('v_project_dict')['a'] > 10 %}
      concat(c_string, c_float) AS c_if,
    {% elif v_dict['a'] > 10 or   var('v_project_dict')['b'] == 'hello' %}
      concat(c_string, c_int) AS c_if,
    {% else %}
      concat(c_string, c_bigint) AS c_if,
    {% endif %}
    {% for c_i in range(0, 5) %}
      concat(c_string, {{c_i}}) AS cfor_col_{{c_i}}
    {% if not loop.last %} , {% endif %}
    {% endfor %}
  
  FROM env_uitesting_shared_parent_model_1 AS in0

),

Macro_1 AS (

  {{ SQL_DatabricksSharedBasic.qa_select_all_not_null_gem(model = 'env_uitesting_shared_parent_model_1', col = 'c_string') }}

),

SQLStatement_1 AS (

  SELECT *
  
  FROM Macro_1 AS in0
  
  WHERE c_tinyint > -1

),

Limit_1 AS (

  SELECT * 
  
  FROM Reformat_1 AS in0
  
  LIMIT 10

),

Join_1 AS (

  SELECT 
    in0.c_tinyint AS c_tinyint,
    in1.c_smallint AS c_smallint,
    in0.c_int AS c_int,
    in1.c_bigint AS c_bigint,
    in0.c_float AS c_float,
    in0.c_double AS c_double,
    in0.c_string AS c_string,
    in0.c_boolean AS c_boolean,
    in0.c_array AS c_array,
    in0.c_struct AS c_struct
  
  FROM SQLStatement_1 AS in0
  INNER JOIN Limit_1 AS in1
     ON in0.c_tinyint = in1.c_tinyint

),

raw_customers AS (

  SELECT * 
  
  FROM {{ ref('raw_customers')}}

),

Reformat_2 AS (

  SELECT * 
  
  FROM raw_customers AS in0

)

SELECT *

FROM Join_1
