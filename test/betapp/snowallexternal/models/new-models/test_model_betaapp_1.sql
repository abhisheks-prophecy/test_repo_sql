{{
  config({    
    "materialized": "table"
  })
}}

{% set v_model_int = 10 %}
{% set v_model_dict = { 'a' : 10 } %}


WITH raw_customers AS (

  SELECT * 
  
  FROM {{ ref('raw_customers')}}

),

accepted_values_complex_1 AS (

  {{ SnowSimpleSchemaProject.accepted_values_complex(column_name = 'first_name', model = 'customers', values = ['Nathan']) }}

),

Reformat_3 AS (

  SELECT * 
  
  FROM accepted_values_complex_1 AS in0

),

qa_customers_all_above_given_id_1 AS (

  {{ SnowSimpleSchemaProject.qa_customers_all_above_given_id() }}

),

raw_orders AS (

  SELECT * 
  
  FROM {{ ref('raw_orders')}}

),

OrderBy_1 AS (

  SELECT * 
  
  FROM raw_orders AS in0
  
  ORDER BY order_date ASC NULLS FIRST

),

Join_6 AS (

  SELECT 
    in1.ID AS ID,
    in1.USER_ID AS USER_ID,
    in1.ORDER_DATE AS ORDER_DATE,
    in1.STATUS AS STATUS
  
  FROM qa_customers_all_above_given_id_1 AS in0
  INNER JOIN OrderBy_1 AS in1
     ON in0.id = in1.id

),

Join_2 AS (

  SELECT 
    in0.ID AS ID,
    in0.USER_ID AS USER_ID,
    in0.ORDER_DATE AS ORDER_DATE,
    in0.STATUS AS STATUS,
    in1.first_name AS first_name,
    in1.last_name AS last_name
  
  FROM Join_6 AS in0
  INNER JOIN raw_customers AS in1
     ON in0.USER_ID = in1.id

),

SQLStatement_1 AS (

  SELECT *
  
  FROM payments

),

Join_3 AS (

  SELECT 
    in1.ID AS ID,
    in1.USER_ID AS USER_ID,
    in1.ORDER_DATE AS ORDER_DATE,
    in1.STATUS AS STATUS,
    in1.FIRST_NAME AS FIRST_NAME,
    in1.LAST_NAME AS LAST_NAME,
    in0.ORDER_ID AS ORDER_ID,
    in0.PAYMENT_METHOD AS PAYMENT_METHOD,
    in0.AMOUNT AS AMOUNT
  
  FROM SQLStatement_1 AS in0
  INNER JOIN Join_2 AS in1
     ON in0.ORDER_ID = in1.ID

),

All_Expr AS (

  {#has all expressions and conditions#}
  SELECT 
    id AS c_int,
    random() + ST_PERIMETER(TO_GEOGRAPHY('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')) + ST_HAUSDORFFDISTANCE(ST_POINT(0, 0), ST_POINT(0, 1)) + EXTRACT(YEAR FROM TO_TIMESTAMP('2013-05-08T23:39:20.123-07:00')) + DATE_PART(QUARTER, '2013-05-08'::DATE) + abs(-10) + ceil(10.12) + floor(12.5656) + mod(10, 2) + round(-975.975, 1) + SIGN(-1.35E-10) + truncate(4.23423) + truncate(4.23423, 2) + cbrt(8) + exp(2) + factorial(1) + pow(2, 3) + power(1, 2) + sqrt(4) + square(2) + ln(10) + log(10, 10) + COS(0) + COS(PI() / 3) + COS(RADIANS(90)) + SIN(0) + SIN(PI() / 3) + SIN(RADIANS(90)) - HAVERSINE(40.7127, -74.0059, 34.05, -118.25) + DAYOFMONTH('2013-05-08T23:39:20.123-07:00'::TIMESTAMP) AS c_number,
    concat(TRIM('❄-❄ABC-❄-', '❄-'), REPLACE('abcd', 'bc'), RIGHT('ABCDEFG', 3), CAST(HASH(SEQ8()) AS string), ASCII('A'), REPEAT('xy', 5), REVERSE('Hello, world!'), SUBSTR('testing 1 2 3', 9, 5), INSERT('abc', 1, 2, 'Z'), RTRIM('$125.00', '0.'), UUID_STRING(), sha1('Snowflake'), CAST(md5_binary('Snowflake') AS string), LPAD(' hello ', 10, ' '), DECOMPRESS_STRING(TO_BINARY('0920536E6F77666C616B65', 'HEX'), 'SNAPPY'), LPAD('.  hi. ', 10, '$'), DAYNAME(TO_DATE('2015-05-01')), CAST(LAST_DAY(TO_DATE('2015-05-08T23:39:20.123-07:00')) AS string), CAST(DATEADD(YEAR, 2, TO_DATE('2013-05-08')) AS string), CAST(DATEDIFF(month, '2021-01-01'::DATE, '2021-02-28'::DATE) AS string), CAST(DATEDIFF(HOUR, '2013-05-08T23:39:20.123-07:00'::TIMESTAMP, DATEADD(YEAR, 2, ('2013-05-08T23:39:20.123-07:00')::TIMESTAMP)) AS string), CAST(TIMEDIFF(YEAR, '2017-01-01', '2019-01-01') AS string), randstr(abs(random()) % 10, random()), CAST(TIME_SLICE('2019-02-28'::DATE, 4, 'MONTH', 'START') AS string), CAST(TRY_TO_TIME('12:30:00') AS string)) AS c_string,
    SPLIT('127.0.0.1', '.') AS c_split,
    2 = 5 or 5 != 10 or 6 <> 7 or 4 > 2 or 5 <= 10 or startswith('sasd', 'te') or REGEXP_LIKE('sanson', 'san.*') or RLIKE('city', 'san.*', 'i') or CONTAINS('hello', 'te') or ('subject' LIKE '%j%h%do%') or (BITNOT(10) = 2) or BITAND(1, 2) = 2 or BITOR(3, 4) = 5 or BITXOR(7, 8) = 4 or GETBIT(11, 100) = 0 or (1.35 BETWEEN 1 and 2) or BOOLAND(1, -2) or  BOOLNOT(10) or BOOLOR(-1.35, 0) or BOOLXOR(1, -1) or (COALESCE(1, 2, 3) = 2) or (decode(1, 1, 'one', 2, 'two', NULL, '-NULL-', 'other') = 'one') or EQUAL_NULL(1, 1) or (GREATEST(1, 2, 3) = 3) or iff(True, 'true', 'false') or ifnull(0, 1) = 0 or (NULL IN (1, 2, NULL)) or (NULL NOT IN (1, 2, NULL)) or (1 IS NOT DISTINCT FROM 1) or (1 IS NOT NULL) or LEAST(1, 3, 0, 4) = 0 or NULLIF(1, 2) = 0 or NULLIFZERO(0) = NULL or NVL('food', 'bard') = 'food' or NVL2(2, 3, 5) = 5 or REGR_VALX(NULL, 10) = NULL or REGR_VALY(NULL, 10) = NULL or ZEROIFNULL(1.0) = 10 or (CURRENT_CLIENT() LIKE '%Snow%') or (CAST(CURRENT_TIME(2) AS string) LIKE '%2020%') or (LOCALTIMESTAMP() = CURRENT_TIMESTAMP) or (CURRENT_WAREHOUSE() != CURRENT_SCHEMA()) or (CURRENT_USER() = 'Abhishek') or TRY_CAST('ABCD' AS VARCHAR (10)) = 'ABCD' or TRY_TO_TIMESTAMP('Invalid') = NULL or TO_ARRAY(1) = TO_ARRAY(1) or PARSE_JSON('{"a":1}') = PARSE_JSON('{"a":1}') or TO_OBJECT(PARSE_JSON('{"a":1}')) = TO_OBJECT(PARSE_JSON('{"a":1}')) or TO_VARIANT(3.14) = TO_VARIANT(3.14) or (TRY_TO_GEOGRAPHY('Not a valid input for this data type.') IS NULL) or (random() > 10) or normal(0, 1, random()) > 10 or uniform(1, 10, random()) = 10 or zipf(1, 10, random()) = 9 or DATE_FROM_PARTS(2010, 1, 100) = DATE_FROM_PARTS(2010, 1, 100) or time_from_parts(0, 100, 0) IS NOT NULL or timestamp_ntz_from_parts(2013, 4, 5, 12, 0, 0, 987654321) IS NOT NULL or DATE_PART(QUARTER, '2013-05-08'::DATE) = 2 or DAYNAME('2013-05-08') IS NOT NULL or EXTRACT(YEAR FROM TO_TIMESTAMP('2013-05-08T23:39:20.123-07:00')) = 2013 or MONTHNAME(TO_TIMESTAMP('2015-04-03 10:00')) IS NOT NULL or PREVIOUS_DAY('2020-10-10', 'Friday ') IS NOT NULL or DAYOFMONTH('2013-05-08T23:39:20.123-07:00'::TIMESTAMP) IS NOT NULL or DAYOFWEEK('2013-05-08T23:39:20.123-07:00'::TIMESTAMP) IS NOT NULL or DAYOFWEEKISO('2013-05-08T23:39:20.123-07:00'::TIMESTAMP) IS NOT NULL or DAYOFYEAR('2013-05-08T23:39:20.123-07:00'::TIMESTAMP) IS NOT NULL or DAY('2013-05-08T23:39:20.123-07:00'::TIMESTAMP) IS NOT NULL or WEEK('2013-05-08T23:39:20.123-07:00'::TIMESTAMP) IS NOT NULL or WEEKISO('2013-05-08T23:39:20.123-07:00'::TIMESTAMP) IS NOT NULL or WEEKOFYEAR('2013-05-08T23:39:20.123-07:00'::TIMESTAMP) IS NOT NULL or MONTH('2013-05-08T23:39:20.123-07:00'::TIMESTAMP) IS NOT NULL or QUARTER('2013-05-08T23:39:20.123-07:00'::TIMESTAMP) IS NOT NULL or ADD_MONTHS('2016-05-15'::timestamp_ntz, 2) IS NOT NULL or DATEADD(MONTH, 1, '2000-01-31'::DATE) IS NOT NULL or DATEDIFF(YEAR, '2010-04-09 14:39:20'::TIMESTAMP, '2013-05-08 23:39:20'::TIMESTAMP) IS NOT NULL or ROUND(MONTHS_BETWEEN('2019-03-31 12:00:00'::TIMESTAMP, '2019-02-28 00:00:00'::TIMESTAMP)) IS NOT NULL or DATEADD(HOUR, 2, TO_TIMESTAMP_LTZ('2013-05-08 11:22:33.444')) IS NOT NULL or DATE_TRUNC('HOUR', TO_TIMESTAMP('2015-05-08T23:39:20.123-07:00')) IS NOT NULL or TIME_SLICE('2019-02-28'::DATE, 4, 'MONTH', 'START') IS NOT NULL or trunc(to_date('2013-05-08'), 'QUARTER') IS NOT NULL or ST_PERIMETER(TO_GEOGRAPHY('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')) IS NOT NULL or ST_DWITHIN(ST_MAKEPOINT(0, 0), ST_MAKEPOINT(1, 0), 150000) or ST_DISJOINT(TO_GEOGRAPHY('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), TO_GEOGRAPHY('POLYGON((3 3, 5 3, 5 5, 3 5, 3 3))')) or ST_STARTPOINT(TO_GEOMETRY('LINESTRING(1 1, 2 2, 3 3, 4 4)')) IS NOT NULL or ST_ENDPOINT(TO_GEOGRAPHY('LINESTRING(1 1, 2 2, 3 3, 4 4)')) IS NOT NULL or ST_SIMPLIFY(TO_GEOGRAPHY('LINESTRING(-122.306067 37.55412, -122.32328 37.561801, -122.325879 37.586852)'), 1000) IS NOT NULL or ST_INTERSECTION(TO_GEOGRAPHY('POLYGON((0 0, 1 0, 2 1, 1 2, 2 3, 1 4, 0 4, 0 0))'), TO_GEOGRAPHY('POINT(0 2)')) IS NOT NULL or ST_GEOGFROMGEOHASH('9q9j8ue2v71y5zzy0s4q') IS NOT NULL or ST_AREA(ST_MAKEPOLYGONORIENTED(TO_GEOGRAPHY('LINESTRING(0.0 0.0, 1.0 0.0, 1.0 2.0, 0.0 2.0, 0.0 0.0)'))) > 20 or ST_GEOGRAPHYFROMWKT('POINT(-122.35 37.55)') IS NOT NULL or ST_XMAX(TO_GEOGRAPHY('POINT(-180 0)')) IS NOT NULL or ST_XMIN(TO_GEOGRAPHY('POINT(-180 0)')) IS NOT NULL or ST_POINTN(TO_GEOGRAPHY('LINESTRING(1 1, 2 2, 3 3, 4 4)'), 2) IS NOT NULL or ST_BUFFER(TO_GEOMETRY('POINT(0 0)'), 1) IS NOT NULL or ST_CENTROID(TO_GEOGRAPHY('LINESTRING(0 0, 0 -2)')) IS NOT NULL or ST_INTERSECTION(TO_GEOGRAPHY('POLYGON((0 0, 1 0, 2 1, 1 2, 2 3, 1 4, 0 4, 0 0))'), TO_GEOGRAPHY('POLYGON((3 0, 3 4, 2 4, 1 3, 2 2, 1 1, 2 0, 3 0))')) IS NOT NULL or ST_SYMDIFFERENCE(TO_GEOGRAPHY('POLYGON((0 0, 1 0, 2 1, 1 2, 2 3, 1 4, 0 4, 0 0))'), TO_GEOGRAPHY('POLYGON((3 0, 3 4, 2 4, 1 3, 2 2, 1 1, 2 0, 3 0))')) IS NOT NULL or ST_UNION(TO_GEOGRAPHY('POINT(1 1)'), TO_GEOGRAPHY('LINESTRING(1 0, 1 2)')) IS NOT NULL or CHECK_XML('<name> Valid </name>') IS NOT NULL or CHECK_JSON('{"a": 2}') IS NOT NULL or JSON_EXTRACT_PATH_TEXT('{"level_1_key": {"level_2_key": "level_2_value"}}', 'level_1_key') IS NOT NULL or PARSE_JSON('null') IS NOT NULL or ARRAY_APPEND(ARRAY_CONSTRUCT(1, 2, 3), 'HELLO') IS NOT NULL or ARRAY_CAT(ARRAY_CONSTRUCT(1, 2), ARRAY_CONSTRUCT(1, 2)) IS NOT NULL or ARRAY_COMPACT(ARRAY_CONSTRUCT(1, 2)) IS NOT NULL or ARRAY_CONTAINS('hello'::variant, array_construct('hello', 'hi')) or ARRAY_DISTINCT(['A', 'A', 'B', NULL, NULL]) IS NOT NULL or ARRAY_INSERT(ARRAY_CONSTRUCT(0, 1, 2, 3), 2, 'hello') IS NOT NULL or array_intersection(ARRAY_CONSTRUCT('A', 'B'), ARRAY_CONSTRUCT('B', 'C')) IS NOT NULL or ARRAY_PREPEND(ARRAY_CONSTRUCT(0, 1, 2, 3), 'hello') IS NOT NULL or ARRAY_SIZE(ARRAY_CONSTRUCT(1, 2, 3)) > 2 or array_slice(array_construct(0, 1, 2, 3, 4, 5, 6), 0, 2) IS NOT NULL or ARRAY_TO_STRING(PARSE_JSON(NULL), '') IS NULL or ARRAYS_OVERLAP(array_construct('hello', 'aloha'), array_construct('hello', 'hi', 'hey')) or OBJECT_CONSTRUCT('a', 1, 'b', 'BBBB', 'c', NULL) IS NOT NULL or OBJECT_DELETE(OBJECT_CONSTRUCT('a', 1, 'b', 2, 'c', 3), 'a', 'b') IS NOT NULL or OBJECT_INSERT(OBJECT_CONSTRUCT('a', 1, 'b', 2), 'c', 3) IS NOT NULL or OBJECT_PICK(OBJECT_CONSTRUCT('a', 1, 'b', 2, 'c', 3), 'a', 'b') IS NOT NULL or TO_ARRAY(1) IS NOT NULL or AS_DECIMAL(TO_VARIANT(TO_DECIMAL(1.23, 6, 3)), 6, 3) IS NOT NULL or typeof(10) IS NOT NULL AS c_boolean,
    ST_GEOGFROMGEOHASH('9q9j8ue2v71y5zzy0s4q') AS c_geo1,
    ST_GEOGRAPHYFROMWKT('POINT(-122.35 37.55)') AS c_geo2,
    TO_GEOGRAPHY('POINT(-122.35 37.55)') AS c_geo3,
    ST_INTERSECTION(TO_GEOGRAPHY('POLYGON((0 0, 1 0, 2 1, 1 2, 2 3, 1 4, 0 4, 0 0))'), TO_GEOGRAPHY('POINT(0 2)')) AS c_geo4,
    ST_SIMPLIFY(TO_GEOGRAPHY('LINESTRING(-122.306067 37.55412, -122.32328 37.561801, -122.325879 37.586852)'), 1000) AS c_geo5,
    CASE ('test_string')
      WHEN 'first choice'
        THEN 'one'
      WHEN 'second choice2'
        THEN 'two'
      WHEN 'first choice1'
        THEN 'one'
      WHEN 'first choice2'
        THEN 'one'
      WHEN 'first choice3'
        THEN 'one'
      WHEN 'first choice4'
        THEN 'one'
      WHEN 'first choice5'
        THEN 'one'
      WHEN 'first choicea'
        THEN 'one'
      WHEN 'first choiceas'
        THEN 'one'
      WHEN 'first choiasdce'
        THEN 'one'
      WHEN 'first ch3oice'
        THEN 'one'
      WHEN 'first choiasasce'
        THEN 'one'
      WHEN 'first chofice'
        THEN 'one'
      WHEN 'first chfoice'
        THEN 'one'
      WHEN 'first choice'
        THEN 'one'
      WHEN 'second choice2'
        THEN 'two'
      WHEN 'first choice1'
        THEN 'one'
      WHEN 'first choice2'
        THEN 'one'
      WHEN 'first choice3'
        THEN 'one'
      WHEN 'first choice4'
        THEN 'one'
      WHEN 'first choice5'
        THEN 'one'
      WHEN 'first choicea'
        THEN 'one'
      WHEN 'first choiceas'
        THEN 'one'
      WHEN 'first choiasdce'
        THEN 'one'
      WHEN 'first ch3oice'
        THEN 'one'
      WHEN 'first choiasasce'
        THEN 'one'
      WHEN 'first chofice'
        THEN 'one'
      WHEN 'first chfoice'
        THEN 'one'
      WHEN 'first choice'
        THEN 'one'
      WHEN 'second choice2'
        THEN 'two'
      WHEN 'first choice1'
        THEN 'one'
      WHEN 'first choice2'
        THEN 'one'
      WHEN 'first choice3'
        THEN 'one'
      WHEN 'first choice4'
        THEN 'one'
      WHEN 'first choice5'
        THEN 'one'
      WHEN 'first choicea'
        THEN 'one'
      WHEN 'first choiceas'
        THEN 'one'
      WHEN 'first choiasdce'
        THEN 'one'
      WHEN 'first ch3oice'
        THEN 'one'
      WHEN 'first choiasasce'
        THEN 'one'
      WHEN 'first chofice'
        THEN 'one'
      WHEN 'first chfoice'
        THEN 'one'
      WHEN 'first choice'
        THEN 'one'
      WHEN 'second choice2'
        THEN 'two'
      WHEN 'first choice1'
        THEN 'one'
      WHEN 'first choice2'
        THEN 'one'
      WHEN 'first choice3'
        THEN 'one'
      WHEN 'first choice4'
        THEN 'one'
      WHEN 'first choice5'
        THEN 'one'
      WHEN 'first choicea'
        THEN 'one'
      WHEN 'first choiceas'
        THEN 'one'
      WHEN 'first choiasdce'
        THEN 'one'
      WHEN 'first ch3oice'
        THEN 'one'
      WHEN 'first choiasasce'
        THEN 'one'
      WHEN 'first chofice'
        THEN 'one'
      WHEN 'first chfoice'
        THEN 'one'
      WHEN 'first choice'
        THEN 'one'
      WHEN 'second choice2'
        THEN 'two'
      WHEN 'first choice1'
        THEN 'one'
      WHEN 'first choice2'
        THEN 'one'
      WHEN 'first choice3'
        THEN 'one'
      WHEN 'first choice4'
        THEN 'one'
      WHEN 'first choice5'
        THEN 'one'
      WHEN 'first choicea'
        THEN 'one'
      WHEN 'first choiceas'
        THEN 'one'
      WHEN 'first choiasdce'
        THEN 'one'
      WHEN 'first ch3oice'
        THEN 'one'
      WHEN 'first choiasasce'
        THEN 'one'
      WHEN 'first chofice'
        THEN 'one'
      WHEN 'first chfoice'
        THEN 'one'
      WHEN 'first choice'
        THEN 'one'
      WHEN 'second choice2'
        THEN 'two'
      WHEN 'first choice1'
        THEN 'one'
      WHEN 'first choice2'
        THEN 'one'
      WHEN 'first choice3'
        THEN 'one'
      WHEN 'first choice4'
        THEN 'one'
      WHEN 'first choice5'
        THEN 'one'
      WHEN 'first choicea'
        THEN 'one'
      WHEN 'first choiceas'
        THEN 'one'
      WHEN 'first choiasdce'
        THEN 'one'
      WHEN 'first ch3oice'
        THEN 'one'
      WHEN 'first choiasasce'
        THEN 'one'
      WHEN 'first chofice'
        THEN 'one'
      WHEN 'first chfoice'
        THEN 'one'
      WHEN 'first choice'
        THEN 'one'
      WHEN 'second choice2'
        THEN 'two'
      WHEN 'first choice1'
        THEN 'one'
      WHEN 'first choice2'
        THEN 'one'
      WHEN 'first choice3'
        THEN 'one'
      WHEN 'first choice4'
        THEN 'one'
      WHEN 'first choice5'
        THEN 'one'
      WHEN 'first choicea'
        THEN 'one'
      WHEN 'first choiceas'
        THEN 'one'
      WHEN 'first choiasdce'
        THEN 'one'
      WHEN 'first ch3oice'
        THEN 'one'
      WHEN 'first choiasasce'
        THEN 'one'
      WHEN 'first chofice'
        THEN 'one'
      WHEN 'first chfoice'
        THEN 'one'
      WHEN 'first choice'
        THEN 'one'
      WHEN 'second choice2'
        THEN 'two'
      WHEN 'first choice1'
        THEN 'one'
      WHEN 'first choice2'
        THEN 'one'
      WHEN 'first choice3'
        THEN 'one'
      WHEN 'first choice4'
        THEN 'one'
      WHEN 'first choice5'
        THEN 'one'
      WHEN 'first choicea'
        THEN 'one'
      WHEN 'first choiceas'
        THEN 'one'
      WHEN 'first choiasdce'
        THEN 'one'
      WHEN 'first ch3oice'
        THEN 'one'
      WHEN 'first choiasasce'
        THEN 'one'
      WHEN 'first chofice'
        THEN 'one'
      WHEN 'first chfoice'
        THEN 'one'
      WHEN 'first choice'
        THEN 'one'
      WHEN 'second choice2'
        THEN 'two'
      WHEN 'first choice1'
        THEN 'one'
      WHEN 'first choice2'
        THEN 'one'
      WHEN 'first choice3'
        THEN 'one'
      WHEN 'first choice4'
        THEN 'one'
      WHEN 'first choice5'
        THEN 'one'
      WHEN 'first choicea'
        THEN 'one'
      WHEN 'first choiceas'
        THEN 'one'
      WHEN 'first choiasdce'
        THEN 'one'
      WHEN 'first ch3oice'
        THEN 'one'
      WHEN 'first choiasasce'
        THEN 'one'
      WHEN 'first chofice'
        THEN 'one'
      WHEN 'first chfoice'
        THEN 'one'
      WHEN 'first choice'
        THEN 'one'
      WHEN 'second choice2'
        THEN 'two'
      WHEN 'first choice1'
        THEN 'one'
      WHEN 'first choice2'
        THEN 'one'
      WHEN 'first choice3'
        THEN 'one'
      WHEN 'first choice4'
        THEN 'one'
      WHEN 'first choice5'
        THEN 'one'
      WHEN 'first choicea'
        THEN 'one'
      WHEN 'first choiceas'
        THEN 'one'
      WHEN 'first choiasdce'
        THEN 'one'
      WHEN 'first ch3oice'
        THEN 'one'
      WHEN 'first choiasasce'
        THEN 'one'
      WHEN 'first chofice'
        THEN 'one'
      WHEN 'first chfoice'
        THEN 'one'
      WHEN 'first choice'
        THEN 'one'
      WHEN 'second choice2'
        THEN 'two'
      WHEN 'first choice1'
        THEN 'one'
      WHEN 'first choice2'
        THEN 'one'
      WHEN 'first choice3'
        THEN 'one'
      WHEN 'first choice4'
        THEN 'one'
      WHEN 'first choice5'
        THEN 'one'
      WHEN 'first choicea'
        THEN 'one'
      WHEN 'first choiceas'
        THEN 'one'
      WHEN 'first choiasdce'
        THEN 'one'
      WHEN 'first ch3oice'
        THEN 'one'
      WHEN 'first choiasasce'
        THEN 'one'
      WHEN 'first chofice'
        THEN 'one'
      WHEN 'first chfoice'
        THEN 'one'
      WHEN 'first choice'
        THEN 'one'
      WHEN 'second choice2'
        THEN 'two'
      WHEN 'first choice1'
        THEN 'one'
      WHEN 'first choice2'
        THEN 'one'
      WHEN 'first choice3'
        THEN 'one'
      WHEN 'first choice4'
        THEN 'one'
      WHEN 'first choice5'
        THEN 'one'
      WHEN 'first choicea'
        THEN 'one'
      WHEN 'first choiceas'
        THEN 'one'
      WHEN 'first choiasdce'
        THEN 'one'
      WHEN 'first ch3oice'
        THEN 'one'
      WHEN 'first choiasasce'
        THEN 'one'
      WHEN 'first chofice'
        THEN 'one'
      WHEN 'first chfoice'
        THEN 'one'
      WHEN 'first choice'
        THEN 'one'
      WHEN 'second choice2'
        THEN 'two'
      WHEN 'first choice1'
        THEN 'one'
      WHEN 'first choice2'
        THEN 'one'
      WHEN 'first choice3'
        THEN 'one'
      WHEN 'first choice4'
        THEN 'one'
      WHEN 'first choice5'
        THEN 'one'
      WHEN 'first choicea'
        THEN 'one'
      WHEN 'first choiceas'
        THEN 'one'
      WHEN 'first choiasdce'
        THEN 'one'
      WHEN 'first ch3oice'
        THEN 'one'
      WHEN 'first choiasasce'
        THEN 'one'
      WHEN 'first chofice'
        THEN 'one'
      WHEN 'first chfoice'
        THEN 'one'
      WHEN 'first choice'
        THEN 'one'
      WHEN 'second choice2'
        THEN 'two'
      WHEN 'first choice1'
        THEN 'one'
      WHEN 'first choice2'
        THEN 'one'
      WHEN 'first choice3'
        THEN 'one'
      WHEN 'first choice4'
        THEN 'one'
      WHEN 'first choice5'
        THEN 'one'
      WHEN 'first choicea'
        THEN 'one'
      WHEN 'first choiceas'
        THEN 'one'
      WHEN 'first choiasdce'
        THEN 'one'
      WHEN 'first ch3oice'
        THEN 'one'
      WHEN 'first choiasasce'
        THEN 'one'
      WHEN 'first chofice'
        THEN 'one'
      WHEN 'first chfoice'
        THEN 'one'
      WHEN 'first choice'
        THEN 'one'
      WHEN 'second choice2'
        THEN 'two'
      WHEN 'first choice1'
        THEN 'one'
      WHEN 'first choice2'
        THEN 'one'
      WHEN 'first choice3'
        THEN 'one'
      WHEN 'first choice4'
        THEN 'one'
      WHEN 'first choice5'
        THEN 'one'
      WHEN 'first choicea'
        THEN 'one'
      WHEN 'first choiceas'
        THEN 'one'
      WHEN 'first choiasdce'
        THEN 'one'
      WHEN 'first ch3oice'
        THEN 'one'
      WHEN 'first choiasasce'
        THEN 'one'
      WHEN 'first chofice'
        THEN 'one'
      WHEN 'first chfoice'
        THEN 'one'
      ELSE 'unexpected choice'
    END AS c_case,
    TO_VARIANT(PARSE_JSON('{"key3": "value3", "key4": "value4"}')) AS c_variant,
    OBJECT_CONSTRUCT('name', 'Jones'::VARIANT, 'age', 42::VARIANT) AS c_object_construct,
    {{ SnowSimpleSchemaProject.qa_concat_macro('STATUS') }} AS c_macro_concat
  
  FROM Join_3 AS in0

),

Reformat_2 AS (

  SELECT 
    id AS c_number,
    concat(first_name, last_name) AS c_string,
    {{ SnowSimpleSchemaProject.round_function('id', 2) }} AS c_macro
  
  FROM raw_customers AS in0

),

ALL_TYPE_TABLE_SMALLER AS (

  SELECT * 
  
  FROM {{ source('QA_DATABASE.qa_simple_schema', 'ALL_TYPE_TABLE_SMALLER') }}

),

Reformat_1 AS (

  SELECT 
    C_NUM AS C_NUM,
    C_NUM10 AS C_NUM10,
    C_DEC AS C_DEC,
    C_NUMERIC AS C_NUMERIC,
    C_INT AS C_INT,
    C_INTEGER AS C_INTEGER,
    C_DOUBLE AS C_DOUBLE,
    C_FLOAT AS C_FLOAT,
    C_COUBLE_PRECISION AS C_COUBLE_PRECISION,
    C_REAL AS C_REAL,
    C_VARCHAR AS C_VARCHAR,
    C_VARCHAR50 AS C_VARCHAR50,
    C_CHAR AS C_CHAR,
    C_CHAR10 AS C_CHAR10,
    C_STRING AS C_STRING,
    C_STRING20 AS C_STRING20,
    C_TEXT AS C_TEXT,
    C_TEXT30 AS C_TEXT30,
    C_BINARY AS C_BINARY,
    C_BINARY100 AS C_BINARY100,
    C_VARBINARY AS C_VARBINARY,
    C_BOOL AS C_BOOL,
    C_TIMESTAMP AS C_TIMESTAMP,
    C_DATE AS C_DATE,
    C_DATETIME AS C_DATETIME,
    C_TIME AS C_TIME,
    C_ARRAY AS C_ARRAY,
    C_OBJECT AS C_OBJECT,
    C_GEOGRAPHY AS C_GEOGRAPHY
  
  FROM ALL_TYPE_TABLE_SMALLER AS in0

),

Join_1 AS (

  SELECT 
    in1.C_NUM AS C_NUM,
    in1.C_NUM10 AS C_NUM10,
    in1.C_DEC AS C_DEC,
    in1.C_NUMERIC AS C_NUMERIC,
    in1.C_INT AS C_INT,
    in1.C_INTEGER AS C_INTEGER,
    in1.C_DOUBLE AS C_DOUBLE,
    in1.C_FLOAT AS C_FLOAT,
    in1.C_COUBLE_PRECISION AS C_COUBLE_PRECISION,
    in1.C_REAL AS C_REAL,
    in1.C_VARCHAR AS C_VARCHAR,
    in1.C_VARCHAR50 AS C_VARCHAR50,
    in1.C_CHAR AS C_CHAR,
    in1.C_CHAR10 AS C_CHAR10,
    in1.C_STRING AS C_STRING,
    in1.C_STRING20 AS C_STRING20,
    in1.C_TEXT AS C_TEXT,
    in1.C_TEXT30 AS C_TEXT30,
    in1.C_BINARY AS C_BINARY,
    in1.C_BINARY100 AS C_BINARY100,
    in1.C_VARBINARY AS C_VARBINARY,
    in1.C_BOOL AS C_BOOL,
    in1.C_TIMESTAMP AS C_TIMESTAMP,
    in1.C_DATE AS C_DATE,
    in1.C_DATETIME AS C_DATETIME,
    in1.C_TIME AS C_TIME,
    in1.C_ARRAY AS C_ARRAY,
    in1.C_OBJECT AS C_OBJECT,
    in1.C_GEOGRAPHY AS C_GEOGRAPHY
  
  FROM Reformat_2 AS in0
  INNER JOIN Reformat_1 AS in1
     ON in0.C_NUMBER != in1.C_NUM

),

Filter_1 AS (

  SELECT * 
  
  FROM Join_1 AS in0
  
  WHERE C_NUM > -1 and C_NUMERIC > 0 and C_STRING NOT LIKE '%asdasdwer345?%'

),

If_For AS (

  {#Contains if for conditions#}
  SELECT 
    c_int AS c_int,
    {% if v_model_int > 10 %}
      c_int AS c_if,
    {% elif v_model_dict['a'] == 10 or   var('v_project_dict')['a'] == 12 %}
      C_NUMERIC AS c_if,
    {% else %}
      C_NUM10 AS c_if,
    {% endif %}
    {% for v_i in range(5) %}
      c_num * {{v_i}} AS c_for_{{v_i}},
    {% endfor %}
    
    C_VARCHAR AS C_VARCHAR,
    C_VARCHAR50 AS C_VARCHAR50,
    C_CHAR AS C_CHAR,
    C_STRING AS C_STRING
  
  FROM Filter_1 AS in0

),

Join_4 AS (

  SELECT 
    in0.C_INT AS C_INT
  
  FROM All_Expr AS in0
  INNER JOIN If_For AS in1
     ON in0.C_INT != in1.C_INT

),

test_not_null_qa_1 AS (

  {{ SnowSimpleSchemaProject.test_not_null_qa(model = 'raw_customers', column_name = 'raw_customers.id') }}

),

Aggregate_1 AS (

  SELECT 
    any_value(C_NUM) AS C_NUM,
    any_value(C_INT) AS C_INT
  
  FROM Filter_1 AS in0
  
  GROUP BY C_BOOL

),

Join_5 AS (

  {#this is my join5 buddy#}
  SELECT 
    in0.C_INT AS C_INT,
    in1.C_NUM AS C_NUM
  
  FROM Join_4 AS in0
  INNER JOIN Aggregate_1 AS in1
     ON in0.C_INT != in1.C_INT

)

SELECT * 

FROM Join_5
