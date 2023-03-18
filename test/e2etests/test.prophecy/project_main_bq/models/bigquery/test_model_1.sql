{{
  config({    
    "materialized": "view"
  })
}}

{% set v_model_c1_float = 12.12 %}
{% set v_model_c1_string = 'hello sir how are you son' %}
{% set v_model_c1_int = 10 %}
{% set v_model_c1_list_int = [1, 2, 3, 4] %}




WITH all_type_table AS (

  SELECT * 
  
  FROM {{ source('prophecy-qa.qa_test_dataset', 'all_type_table') }}

),

qa_table_with_complex_column_names_and_table_with_long_name_1_table_with_complex_column_names_and_table_with_long_name_table_with_complex_column_names_and_table_with_long_name AS (

  SELECT * 
  
  FROM {{ source('prophecy-qa.qa_test_dataset', 'qa_table_with_complex_column_names_and_table_with_long_name_1_table_with_complex_column_names_and_table_with_long_name_table_with_complex_column_names_and_table_with_long_name') }}

),

Join_1 AS (

  SELECT 
    in1.c_int64 + in0.c_int64 AS c_int64,
    {% if v_model_c1_int > 100 and var("v_project_c_int") > 100 %}
      {{ cents_to_dollar(100, 2) }} AS c_bignumeric,
    {% elif v_model_c1_string == 'test string' %}
      {{ cents_to_dollar(100 * 10, 2) }} AS c_bignumeric,
    {% elif var("v_project_c_list_string")[0] == 'hello' or v_model_c1_list_int[0] == 20 %}
      {{ cents_to_dollar(100 * 100, 2) }} AS c_bignumeric,
    {% else %}
      {{ cents_to_dollar(100 * 1000, 2) }} AS c_bignumeric,
    {% endif %}
    in1.c_bool AS c_bool,
    in1.c_bytes AS c_bytes,
    concat(in1.c_string, in0.c_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little123) AS c_string,
    in1.c_float64 AS c_float64,
    in1.c_numeric_1 AS c_numeric_1,
    in1.c_numeric_2 AS c_numeric_2,
    in1.c_date AS c_date,
    in1.c_interval AS c_interval,
    in1.c_time AS c_time,
    in1.c_timestamp AS c_timestamp,
    in1.c_datetime AS c_datetime,
    in1.c_geography AS c_geography,
    in1.c_json AS c_json,
    in1.c_array_int64 AS c_array_int64,
    in1.c_struct AS c_struct,
    in1.p_date AS p_date,
    in0.c_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little123 AS c_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little123,
    in0._c_bool123 AS _c_bool123,
    in0._c_bignumeric AS _c_bignumeric,
    in0.c_interval AS c_interval
  
  FROM qa_table_with_complex_column_names_and_table_with_long_name_1_table_with_complex_column_names_and_table_with_long_name_table_with_complex_column_names_and_table_with_long_name AS in0
  INNER JOIN all_type_table AS in1
     ON in0.c_int64 = in1.c_int64

),

SQLStatement_1 AS (

  WITH Players AS (
  
    SELECT 
      'gorbie' AS username,
      29 AS level,
      'red' AS team
    
    UNION ALL
    
    SELECT 
      'junelyn',
      2,
      'blue'
    
    UNION ALL
    
    SELECT 
      'corba',
      43,
      'green'
  
  ),
  
  NPCs AS (
  
    SELECT 
      'niles' AS username,
      'red' AS team
    
    UNION ALL
    
    SELECT 
      'jujul',
      'red'
    
    UNION ALL
    
    SELECT 
      'effren',
      'blue'
  
  ),
  
  Mascots AS (
  
    SELECT 
      'cardinal' AS mascot,
      'red' AS team
    
    UNION ALL
    
    SELECT 
      'parrot',
      'green'
    
    UNION ALL
    
    SELECT 
      'finch',
      'blue'
    
    UNION ALL
    
    SELECT 
      'sparrow',
      'yellow'
  
  )
  
  SELECT *
  
  FROM (
    SELECT 
      username,
      team
    
    FROM Players
    
    UNION ALL
    
    SELECT 
      username,
      team
    
    FROM NPCs
   )

),

Join_2 AS (

  SELECT 
    in1.c_int64 AS c_int64,
    in1.c_bignumeric AS c_bignumeric,
    in1.c_bool AS c_bool,
    in1.c_bytes AS c_bytes,
    in1.c_string AS c_string,
    in1.c_float64 AS c_float64,
    in1.c_numeric_1 AS c_numeric_1,
    in1.c_numeric_2 AS c_numeric_2,
    in1.c_date AS c_date,
    in1.c_time AS c_time,
    in1.c_timestamp AS c_timestamp,
    in1.c_datetime AS c_datetime,
    in1.c_geography AS c_geography,
    in1.c_json AS c_json,
    in1.c_array_int64 AS c_array_int64,
    in1.c_struct AS c_struct,
    in1.p_date AS p_date,
    in1.c_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little123 AS c_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little123,
    in1._c_bool123 AS _c_bool123,
    in1._c_bignumeric AS _c_bignumeric
  
  FROM SQLStatement_1 AS in0
  INNER JOIN Join_1 AS in1
     ON in0.username != in1.c_string

),

Reformat_2 AS (

  SELECT 
    (10 * 2) / 5 + 10 - 3 + abs(-10) + SIGN(-1) + IEEE_DIVIDE(20, 4.0) + RAND() + SQRT(25.0) + pow(2, 3) - power(2, 3) + exp(1) + ln(1) + log(100, 10) + log10(10) + GREATEST(1, 2, 3, 4, 5.23, 0, -10) + LEAST(1, 2, 3, 4, 5.23, 0, -10) + div(10, 10) + SAFE_DIVIDE(0, 20) + SAFE_MULTIPLY(1, 2) + SAFE_NEGATE(2000) + SAFE_ADD(1, 2) + SAFE_SUBTRACT(10, 20000) + mod(10, 2) + ROUND(2.8) + ROUND(-2.3) + ROUND(NUMERIC "2.25", 1, "ROUND_HALF_EVEN") + ROUND(NUMERIC "-2.5", 0, "ROUND_HALF_AWAY_FROM_ZERO") + trunc(-2.3323) + ceil(-2.8) + ceiling(-2.8) + floor(2.3) + cos(12) + cosh(1) + ACOS(0.123) + ACOSH(1) + COT(1) + SAFE.COT(0) + COTH(1) + SAFE.COTH(1) - CSC(100) + SAFE.CSC(100) + CSCH(0.5) + SEC(100) + SECH(0.5) + SIN(0.2) + SINH(0.2) + ASIN(0.2) + ASINH(0.1) + TAN(1) + TANH(1) + ATAN(1) + ATANH(0.12321) + ATAN2(1, 2) + CBRT(27) + RANGE_BUCKET(20, [0, 10, 20, 30, 40]) + RANGE_BUCKET('a', ['a', 'b', 'c', 'd']) + BYTE_LENGTH('абвгд') + CHAR_LENGTH('asdasdабвгд') + CHARACTER_LENGTH('asdasdабвгд') + INSTR('banana', 'an', 1, 2) + LENGTH('абвгд') + OCTET_LENGTH('sadasd') + REGEXP_INSTR('abhisheks@gmail.com', '@[^.]*') + STRPOS('abhishek@gmail.com', '@') + UNICODE('â') + INT64(JSON_QUERY(JSON '{"gate": "A4", "flight_number": 2}', "$.flight_number")) + FLOAT64(JSON_QUERY(JSON '{"vo2_max": 2.1, "age": 18}', "$.vo2_max")) + ARRAY_LENGTH([1, 2]) + EXTRACT(HOUR FROM DATETIME(2008, 12, 25, 15, 30, 0)) - EXTRACT(WEEK(SUNDAY) FROM DATETIME(TIMESTAMP'2017-11-05 00:00:00+00', "UTC")) + EXTRACT(HOUR FROM TIME "15:30:00") - EXTRACT(MONTH FROM (INTERVAL '1-2 3 4:5:6.789999' YEAR TO SECOND)) AS c_float_complex_expression,
    (10 < 20) or (20 < 30) or (c_int64 <= c_bignumeric) or (20 >= 30) or (10 <> 10) or (20 = 10) or (20 BETWEEN 10 and 21) or ('hello' LIKE '%h\\%\\_\\/%') or (10 in (20, 10, 30)) or ([1, 2, c_int64][OFFSET(0)] > 2) or ((20 >= 30) is TRUE) or ((20 >= 30) is not TRUE) or ((20 >= 30) is FALSE) or ((20 >= 30) is not FALSE) or ((10 > 20) is UNKNOWN) or ((10 > 20) is not UNKNOWN) or ((10 > 20) is not NULL) or ((10 > 20) is NULL) or (COLLATE('Foo', 'und:ci') LIKE COLLATE('%foo%', 'und:ci')) or ('MASSE' LIKE 'Maße') or ('\u3042' LIKE '%\u30A2%') or (CASE
      WHEN c_int64 > 10
        THEN True
      WHEN c_int64 < 10
        THEN False
      ELSE True
    END) or (COALESCE(NULL, 'B', 'C') = 'B') or (IF(c_int64 < c_float64, 'true', 'false') = 'true') or (IFNULL(NULL, 0) = 0) or (NULLIF(10, 0) = 10) or ((ANY_VALUE(c_int64) OVER (ORDER BY LENGTH(c_string) ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) = 0) or (CAST('1' AS string) = '1') or (CAST('1' AS string) = '1') or (CAST('1' AS BIGNUMERIC) = 1) or (CAST('TRUE' AS BOOL) = TRUE) or (CAST(CAST('hi' AS BYTES) AS STRING) = 'hi') or (CAST('2021-04-20' AS DATE) = DATE'2020-09-22') or (CAST('2021-04-20 00:00:00' AS TIMESTAMP) = TIMESTAMP'2020-09-22 00:00:00') or (CAST('23.45' AS FLOAT64) = 23.45) or (CAST('-23' AS INT64) = -23) or (CAST('-0x123' AS INT64) = -291) or (CAST('123' AS NUMERIC) = 123) or (CAST(b'\x48\x65\x6c\x6c\x6f' AS STRING FORMAT 'ASCII') = 'Hello') or (CAST(CURRENT_DATE() AS STRING) = '2020-12-12') or (CAST(CURRENT_DATE() AS STRING FORMAT 'DAY') = 'MONDAY') or (CAST(TIMESTAMP'2008-12-25 00:00:00+00:00' AS STRING FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM') = '2008-12-25 00:00:00+00:00') or (CAST(TIMESTAMP'2008-12-25 00:00:00+00:00' AS STRING FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM' AT TIME ZONE 'Asia/Kolkata') = '2008-12-25 00:00:00+00:00') or ((CAST('18-12-03' AS DATE FORMAT 'YY-MM-DD') = CAST('18-12-03' AS DATE FORMAT 'YY-MM-DD'))) or (CAST('DEC 03, 2018' AS DATE FORMAT 'MON DD, YYYY') = CAST('DEC 03, 2018' AS DATE FORMAT 'MON DD, YYYY')) or (CAST('15:30' AS TIME FORMAT 'HH24:MI') = CAST('15:30' AS TIME FORMAT 'HH24:MI')) or (CAST('03:30 P.M.' AS TIME FORMAT 'HH:MI P.M.') = CAST('03:30 P.M.' AS TIME FORMAT 'HH:MI P.M.')) or (CAST('03:30 P.M.' AS TIME FORMAT 'HH:MI A.M.') = CAST('03:30 P.M.' AS TIME FORMAT 'HH:MI A.M.')) or (CAST(-12345.678 AS STRING FORMAT '$999,999.999') = '-$12,345.678') or (CAST(-123456 AS STRING FORMAT '9.999EEEE') = '-1.235E+05') or (PARSE_BIGNUMERIC("  -  123.45 ") = 123.45) or (PARSE_BIGNUMERIC("  1,2,,3,.45 + ") = 123.25) or (PARSE_NUMERIC("12.34e-1-") = -1.234) or (SAFE_CAST("apple" AS INT64) is NULL) or (ARRAY_TO_STRING(['asd', 'asd'], '--') = 'asd--asd') or (TO_BASE64(b'\377\340') is NULL) or (SAFE_CONVERT_BYTES_TO_STRING(b'\xc2') is NULL) or is_inf(CAST('Infinity' AS float64)) or is_nan(CAST('NaN' AS float64)) or (CONTAINS_SUBSTR('the blue house', 'Blue house')) or (CONTAINS_SUBSTR((23, 35, 41), '35')) or (CONTAINS_SUBSTR(('abc', ['def', 'ghi', 'jkl'], 'mno'), 'jk')) or (CONTAINS_SUBSTR(JSON '{"lunch":"soup"}', "lunch", json_scope => "JSON_VALUES")) or (CONTAINS_SUBSTR(JSON '{"lunch":"soup"}', "lunch", json_scope => "JSON_KEYS_AND_VALUES")) or (ENDS_WITH(c_string, 'e')) or (REGEXP_CONTAINS('abhishek@prophecy.io', r'@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+')) or (REGEXP_CONTAINS('abhishek@prophecy.io', r'^([\w.+-]+@foo\.com|[\w.+-]+@bar\.org)$')) or (STARTS_WITH('asdasdasd', 'b')) or (BOOL(JSON_QUERY(JSON '{"hotel class": "5-star", "vacancy": true}', "$.vacancy"))) or (SAFE.FLOAT64(JSON '"strawberry"') is NULL) AS c_bool_complex_expression,
    concat(c_string || ' hi', LEFT('apple', 3), FORMAT('%T', LPAD('abc', 5)), LOWER('asdsa'), CONCAT('#', LTRIM('.  aasd.    '), '#'), CAST(CAST('hi' AS BYTES) AS string), LEAD(c_string) OVER (PARTITION BY c_date ORDER BY c_bool ASC), LAG(c_string) OVER (PARTITION BY c_date ORDER BY c_bool ASC), CAST(FARM_FINGERPRINT(CONCAT(CAST(2 AS STRING), 'apple', CAST(false AS STRING))) AS string), REGEXP_EXTRACT('abhisheks@prophecy.io', r'^[a-zA-Z0-9_.+-]+'), CAST(MD5("Hello World") AS string), INITCAP('hello sir how'), CAST(SHA1("Hello World") AS string), CAST(SHA256("Hello World") AS string), CAST(SHA512("Hello World") AS string), CAST(ASCII('abcd') AS string), CHR(65), CAST(CODE_POINTS_TO_BYTES([65, 98, 67, 100]) AS string), CODE_POINTS_TO_STRING([65, 255, 513, 1024]), COLLATE('a', 'und:ci'), COLLATE('Foo', 'und:ci'), COLLATE('\u3042', 'und:ci'), FORMAT('date: %s!', FORMAT_DATE('%B %d, %Y', DATE'2015-01-02')), FORMAT("%'d", 12345678), FORMAT("%'x", 12345678), FORMAT("%'o", 55555), CAST(FROM_BASE32('MFRGGZDF74======') AS string), CAST(FROM_BASE64('/+A=') AS string), CAST(FROM_HEX('0AF') AS string), NORMALIZE('\u00ea'), NORMALIZE_AND_CASEFOLD('the red bar'), ARRAY_TO_STRING(REGEXP_EXTRACT_ALL('asdqwe asd 2132', '`(.+?)`'), '--'), REGEXP_REPLACE('# Another heading', r'^# ([a-zA-Z0-9\s]+$)', '<h1>\\1</h1>'), REGEXP_SUBSTR('Hello World Helloo', 'H?ello+', 1, 1), REPLACE('apple cobbler', 'pie', 'cobbler'), REPEAT('asd', 2), REVERSE('asdas 324 123@#$%#@$ asd'), RIGHT('example', 3), RPAD('例子 c', 5, '中文'), RTRIM('**sasd 8*8**', '*'), SAFE_CONVERT_BYTES_TO_STRING(b'\xc2'), SOUNDEX('hello'), ARRAY_TO_STRING(SPLIT('h e lllo son', ' '), ' '), SUBSTR('asdsditem', 2), SUBSTRING('asdsditem', 2, 2), TO_BASE32(b'abcde\xFF'), TO_BASE64(b'\377\340'), REPLACE(REPLACE(TO_BASE64(b'\377\340'), '+', '-'), '/', '_'), TO_HEX(b'foobar'), TRANSLATE('This is a cookie', 'sko', 'zku'), TRIM('*   **as**', '*'), TRIM('abaW̊', 'Y̊'), CAST(TRIM(b'apple', b'na\xab') AS string), UPPER('item'), STRING(JSON_QUERY(JSON '{"class":{"students":[{"id":5},{"name":"abhishek"}]}}', "$.class.students[0].name")), STRING(JSON_QUERY(JSON '{"name": "sky", "color": "blue"}', "$.color")), JSON_EXTRACT_SCALAR(JSON '{ "name" : "Jakob", "age" : "6" }', '$.age'), JSON_EXTRACT_SCALAR('{"a.b": {"c": "world"}}', "$['a.b'].c"), JSON_VALUE(JSON '{ "name" : "Jakob", "age" : "6" }', '$.age'), JSON_VALUE(PARSE_JSON('{ "name" : "Jakob", "age" : "6" }'), '$.age'), JSON_TYPE(JSON '{"name": "sky", "color" : "blue"}'), ARRAY_TO_STRING(ARRAY_CONCAT(['1', '2'], ['3', '4'], ['5', '6']), '-'), ARRAY_TO_STRING(ARRAY_REVERSE(['a', 'b']), '-'), CAST(CURRENT_DATE() AS string), CAST(EXTRACT(DAY FROM DATE'2013-12-25') AS string), CAST(DATE(2016, 12, 25) AS string), CAST(DATE(DATETIME '2016-12-25 23:59:59') AS string), CAST((DATE_ADD(DATE'2008-12-25', INTERVAL 5 DAY)) AS string), CAST((DATE_SUB(DATE'2008-12-25', INTERVAL 5 DAY)) AS string), CAST(DATE_FROM_UNIX_DATE(14238) AS string), FORMAT_DATE('%x', DATE'2008-12-25'), FORMAT_DATE('%b %Y', DATE'2008-12-25'), CAST(LAST_DAY(DATE'2008-11-25') AS string), CAST(PARSE_DATE('%A %b %e %Y', 'Thursday Dec 25 2008') AS string), CAST(PARSE_DATE('%Y%m%d', '20081225') AS string), CAST(UNIX_DATE(DATE'2008-12-25') AS string), CAST(CURRENT_DATETIME() AS string), CAST(DATETIME(TIMESTAMP'2008-12-25 05:30:00+00', "America/Los_Angeles") AS string), CAST(DATETIME_ADD(DATETIME "2008-12-25 15:30:00", INTERVAL 10 MINUTE) AS string), CAST(DATETIME_SUB(DATETIME "2008-12-25 15:30:00", INTERVAL 10 MINUTE) AS string), CAST(DATETIME_DIFF(DATETIME "2010-07-07 10:20:00", DATETIME "2008-12-25 15:30:00", DAY) AS string), CAST(DATETIME_TRUNC(DATETIME "2008-12-25 15:30:00", DAY) AS string), CAST(FORMAT_DATETIME("%b-%d-%Y", DATETIME "2008-12-25 15:30:00") AS string), CAST(LAST_DAY(DATETIME '2008-11-25', MONTH) AS string), CAST(LAST_DAY(DATETIME '2008-11-10 15:30:00', WEEK(SUNDAY)) AS string), CAST(LAST_DAY(DATETIME '2008-11-10 15:30:00', WEEK(MONDAY)) AS string), CAST(PARSE_DATETIME("%c", "Thu Dec 25 07:30:00 2008") AS string), CAST(PARSE_DATETIME("%a %b %e %I:%M:%S %Y", "Thu Dec 25 07:30:00 2008") AS string), CAST(PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', '8/30/2018 2:23:38 pm') AS string), CAST(PARSE_DATETIME('%A, %B %e, %Y', 'Wednesday, December 19, 2018') AS string), CAST(CURRENT_TIME() AS string), CAST(TIME(TIMESTAMP'2008-12-25 15:30:00+08', "America/Los_Angeles") AS string), CAST(TIME(15, 30, 0) AS string), CAST(TIME_ADD(TIME "15:30:00", INTERVAL 10 MINUTE) AS string), CAST(TIME_SUB(TIME "15:30:00", INTERVAL 10 MINUTE) AS string), CAST(FORMAT_TIME("%R", TIME "15:30:00") AS string), CAST(PARSE_TIME("%I:%M:%S", "07:30:00") AS string), CAST(PARSE_TIME('%I:%M:%S %p', '2:23:38 pm') AS string), CAST(CURRENT_TIMESTAMP() AS string), STRING(TIMESTAMP'2008-12-25 15:30:00+00', "UTC"), STRING(TIMESTAMP("2008-12-25 15:30:00", "America/Los_Angeles")), CAST(TIMESTAMP_ADD(TIMESTAMP'2008-12-25 15:30:00+00', INTERVAL 10 MINUTE) AS string), CAST(TIMESTAMP_SUB(TIMESTAMP'2008-12-25 15:30:00+00', INTERVAL 10 MINUTE) AS string), CAST(TIMESTAMP_TRUNC(TIMESTAMP'2008-12-25 15:30:00+00', DAY, "UTC") AS string), CAST(FORMAT_TIMESTAMP("%b-%d-%Y", TIMESTAMP'2008-12-25 15:30:00+00') AS string), CAST(PARSE_TIMESTAMP("%a %b %e %I:%M:%S %Y", "Thu Dec 25 07:30:00 2008") AS string), CAST(PARSE_TIMESTAMP("%c", "Thu Dec 25 07:30:00 2008") AS string), CAST(TIMESTAMP_SECONDS(1230219000) AS string), CAST(TIMESTAMP_MILLIS(1230219000000) AS string), CAST(TIMESTAMP_MICROS(1230219000000000) AS string), CAST(UNIX_SECONDS(TIMESTAMP'2008-12-25 15:30:00+00') AS string), CAST(UNIX_MILLIS(TIMESTAMP'2008-12-25 15:30:00+00') AS string), CAST(UNIX_MICROS(TIMESTAMP'2008-12-25 15:30:00+00') AS string), SESSION_USER(), GENERATE_UUID(), CAST(NET.IP_FROM_STRING('48.49.50.51') AS string), CAST(NET.SAFE_IP_FROM_STRING('48.49.50.51') AS string), CAST(NET.IP_TO_STRING(b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01') AS string), CAST(NET.IP_NET_MASK(4, 0) AS string), CAST(NET.IP_TRUNC(b'\xaa\xbb\xcc\xdd', 11) AS string), CAST(NET.IPV4_TO_INT64(b'\x00\x00\x00\x00') AS string)) AS c_string_complex_expression,
    JSON_EXTRACT_ARRAY('{"fruit":[{"apples":5,"oranges":10},{"apples":2,"oranges":4}],"vegetables":[{"lettuce":7,"kale": 8}]}', '$.fruit') AS c_array_string_complex_1,
    JSON_QUERY_ARRAY(JSON '{"fruits":["apples","oranges","grapes"]}', '$.fruits') AS c_array_json_complex_2,
    JSON_EXTRACT_STRING_ARRAY(JSON '{"fruits":["apples","oranges","grapes"]}', '$.fruits') AS c_array_string_complex_3,
    JSON_VALUE_ARRAY(JSON '{"fruits":["apples","oranges","grapes"]}', '$.fruits') AS c_array_string_complex_4,
    GENERATE_ARRAY(0, 10, 3) AS c_array_int_complex_5,
    GENERATE_DATE_ARRAY('2016-10-05', '2016-10-08') AS c_array_date_complex_6,
    GENERATE_DATE_ARRAY('2016-10-05', '2016-10-01', INTERVAL -3 DAY) AS c_array_date_complex_7,
    GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-07 00:00:00', INTERVAL 1 DAY) AS c_array_timestamp_complex_8,
    {% if v_model_c1_int > 10 %}
      concat(c_bool, c_bignumeric) AS c_test_if_expression,
    {% elif var("v_project_c_list_string")[0] == 'hello' %}
      concat(c_bool, c_array_int64) AS c_test_if_expression,
    {% elif v_model_c1_string == 'hello' %}
      concat(c_bool, c_numeric_1) AS c_test_if_expression,
    {% else %}
      concat(c_bool, c_numeric_2) AS c_test_if_expression,
    {% endif %}
    c_int64 AS c_int64,
    c_bignumeric AS c_bignumeric,
    c_bool AS c_bool,
    c_bytes AS c_bytes,
    c_string AS c_string,
    c_float64 AS c_float64,
    c_numeric_1 AS c_numeric_1,
    c_numeric_2 AS c_numeric_2,
    c_date AS c_date,
    c_time AS c_time,
    c_timestamp AS c_timestamp,
    c_datetime AS c_datetime,
    c_geography AS c_geography,
    c_json AS c_json,
    c_array_int64 AS c_array_int64,
    c_struct AS c_struct,
    p_date AS p_date,
    c_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little123 AS c_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little_buddy_string_with_a_very_long_name_little123,
    _c_bool123 AS _c_bool123,
    _c_bignumeric AS _c_bignumeric,
    ST_GEOGFROMTEXT('POINT EMPTY') AS c_geo_from_text,
    ST_SNAPTOGRID(ST_GEOGFROMTEXT('LINESTRING(0 0, 0.05 0, 0.1 0, 0.15 0, 2 0)'), 1),
    ST_PERIMETER(ST_GEOGFROMTEXT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 2, 2 1))')),
    ST_PERIMETER(ST_GEOGFROMTEXT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 2, 2 1))')) AS c_st_perimeter
  
  FROM Join_2 AS in0

),

Filter_1 AS (

  SELECT * 
  
  FROM Reformat_2 AS in0
  
  WHERE c_float_complex_expression > -100000

),

Geo AS (

  SELECT 
    ST_GEOGFROMTEXT('POINT EMPTY') AS c_geo_from_empty,
    ST_ISCLOSED(ST_GEOGFROMTEXT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 2, 2 1))')) AS c_geo_is_closed,
    ST_SIMPLIFY(ST_GEOGFROMTEXT('LINESTRING(0 0, 0.05 0, 0.1 0, 0.15 0, 2 0)'), 1),
    ST_STARTPOINT(ST_GEOGFROMTEXT('LINESTRING(1 1, 2 1, 3 2, 3 3)')),
    ST_SNAPTOGRID(ST_GEOGFROMTEXT('LINESTRING(0 0, 0.05 0, 0.1 0, 0.15 0, 2 0)'), 1),
    ST_X(ST_GEOGPOINT(-122, 47)),
    ST_Y(ST_GEOGPOINT(-122, 47)),
    ST_TOUCHES(ST_GEOGPOINT(-122, 47), ST_GEOGPOINT(-122, 48)),
    ST_WITHIN(ST_GEOGPOINT(-122, 47), ST_GEOGPOINT(-122, 48)),
    ST_MAXDISTANCE(ST_GEOGPOINT(-122, 47), ST_GEOGPOINT(-122, 48)),
    ST_UNION_AGG(ST_GEOGFROMTEXT('LINESTRING(1 1, 2 1, 3 2, 3 3)')),
    ST_UNION(ST_GEOGPOINT(-122, 47), ST_GEOGPOINT(-122, 48)),
    ST_POINTN(ST_GEOGFROMTEXT('LINESTRING(1 1, 2 1, 3 2, 3 3)'), 2),
    ST_PERIMETER(ST_GEOGFROMTEXT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 2, 2 1))')),
    ST_NUMPOINTS(ST_GEOGFROMTEXT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 2, 2 1))')),
    ST_NUMGEOMETRIES(ST_GEOGFROMTEXT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 2, 2 1))')),
    ST_NPOINTS(ST_GEOGFROMTEXT('POLYGON((-125 48, -124 46, -117 46, -117 49, -125 48))')),
    ST_ISEMPTY(ST_GEOGFROMTEXT('POLYGON((-125 48, -124 46, -117 46, -117 49, -125 48))')),
    ST_MAKELINE(ST_GEOGPOINT(-122, 47), ST_GEOGPOINT(-122, 48)),
    ST_LENGTH(ST_GEOGFROMTEXT('POLYGON((-125 48, -124 46, -117 46, -117 49, -125 48))')),
    ST_ISRING(ST_GEOGFROMTEXT('POLYGON((-125 48, -124 46, -117 46, -117 49, -125 48))')),
    ST_ISCOLLECTION(ST_GEOGFROMTEXT('POLYGON((-125 48, -124 46, -117 46, -117 49, -125 48))')),
    ST_GEOGPOINT(-122, 47),
    ST_GEOGFROMTEXT('LINESTRING(1 2, 3 4)'),
    SAFE.S2_CELLIDFROMPOINT(ST_GEOGFROMTEXT('LINESTRING(1 2, 3 4)')),
    SAFE.S2_CELLIDFROMPOINT(ST_GEOGFROMTEXT('LINESTRING(1 2, 3 4)'), level => 10),
    S2_COVERINGCELLIDS(ST_GEOGPOINT(-122, 47), min_level => 12),
    ST_ANGLE(ST_GEOGPOINT(-122, 47), ST_GEOGPOINT(-122, 49), ST_GEOGPOINT(-120, 47)),
    ST_AREA(ST_GEOGFROMTEXT('POLYGON((-125 48, -124 46, -117 46, -117 49, -125 48))')),
    ST_ASBINARY(ST_GEOGFROMTEXT('POLYGON((-125 48, -124 46, -117 46, -117 49, -125 48))')),
    ST_ASGEOJSON(ST_GEOGFROMTEXT('POLYGON((-125 48, -124 46, -117 46, -117 49, -125 48))')),
    ST_ASTEXT(ST_GEOGFROMTEXT('POLYGON((-125 48, -124 46, -117 46, -117 49, -125 48))')),
    ST_AZIMUTH(ST_GEOGPOINT(-122, 47), ST_GEOGPOINT(-122, 44)),
    ST_BOUNDINGBOX(ST_GEOGFROMTEXT('POLYGON((-125 48, -124 46, -117 46, -117 49, -125 48))')),
    ST_NUMPOINTS(ST_BUFFER(ST_GEOGFROMTEXT('POINT(1 2)'), 50, 2)),
    st_NumPoints(ST_BUFFERWITHTOLERANCE(ST_GEOGFROMTEXT('POINT(100 2)'), 100, 1)),
    ST_CENTROID(ST_GEOGFROMTEXT('POLYGON((-125 48, -124 46, -117 46, -117 49, -125 48))')),
    ST_CENTROID_AGG(ST_GEOGFROMTEXT('POLYGON((-125 48, -124 46, -117 46, -117 49, -125 48))')),
    ST_CLOSESTPOINT(ST_GEOGPOINT(-122, 47), ST_GEOGPOINT(122, 47)),
    ST_CLUSTERDBSCAN(ST_GEOGFROMTEXT('MULTIPOINT(1 1, 2 2, 4 4, 5 2)'), 100000.0, 1) OVER (),
    ST_CONTAINS(ST_GEOGFROMTEXT('POLYGON((1 1, 20 1, 10 20, 1 1))'), ST_GEOGPOINT(0, 0)),
    ST_CONVEXHULL(ST_GEOGFROMTEXT('LINESTRING(1 1, 2 2)')),
    ST_COVEREDBY(ST_GEOGPOINT(-122, 47), ST_GEOGPOINT(122, 47)),
    ST_COVERS(ST_GEOGFROMTEXT('POLYGON((1 1, 20 1, 10 20, 1 1))'), ST_GEOGPOINT(1, 1)),
    ST_DIFFERENCE(ST_GEOGFROMTEXT('POLYGON((0 0, 10 0, 10 10, 0 0))'), ST_GEOGFROMTEXT('POLYGON((4 2, 6 2, 8 6, 4 2))')),
    ST_DIMENSION(ST_GEOGFROMTEXT('POLYGON((0 0, 10 0, 10 10, 0 0))')),
    ST_DISJOINT(ST_GEOGPOINT(-122, 47), ST_GEOGPOINT(122, 47)),
    ST_DISTANCE(ST_GEOGPOINT(-122, 47), ST_GEOGPOINT(122, 47)),
    ST_DUMP(ST_GEOGFROMTEXT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 2, 2 1))')),
    ST_DWITHIN(ST_GEOGPOINT(-122, 47), ST_GEOGPOINT(122, 47), 10),
    ST_ENDPOINT(ST_GEOGFROMTEXT('LINESTRING(1 1, 2 1, 3 2, 3 3)')),
    ST_EQUALS(ST_GEOGPOINT(-122, 47), ST_GEOGPOINT(122, 47)),
    ST_EXTENT(ST_GEOGFROMTEXT('POLYGON((172 53, -130 55, -141 70, 172 53))')),
    ST_EXTERIORRING(ST_GEOGFROMTEXT('POLYGON((1 1, 1 10, 5 10, 5 1, 1 1),(2 2, 3 4, 2 4, 2 2))')),
    ST_GEOGFROM('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'),
    ST_GEOGFROMGEOJSON(ST_ASGEOJSON(ST_GEOGFROMTEXT('POLYGON((-125 48, -124 46, -117 46, -117 49, -125 48))'))),
    ST_GEOGFROMWKB(ST_ASBINARY(ST_GEOGFROMTEXT('POLYGON((-125 48, -124 46, -117 46, -117 49, -125 48))'))),
    ST_GEOHASH(ST_GEOGPOINT(-122.35, 47.62), 10),
    ST_GEOMETRYTYPE(ST_GEOGFROMTEXT('GEOMETRYCOLLECTION(MULTIPOINT(-1 2, 0 12), LINESTRING(-2 4, 0 6))')),
    ST_INTERIORRINGS(ST_GEOGFROMTEXT('POLYGON((1 1, 1 10, 5 10, 5 1, 1 1), (2 2.5, 3.5 3, 2.5 2, 2 2.5), (3.5 7, 4 6, 3 3, 3.5 7))')),
    ST_INTERSECTS(ST_GEOGPOINT(-122, 47), ST_GEOGPOINT(122, 47)),
    ST_INTERSECTSBOX(ST_GEOGPOINT(10, 10), 90, 0, -90, 20)
  
  FROM Reformat_2 AS in0

),

OrderBy_1 AS (

  SELECT * 
  
  FROM Filter_1 AS in0
  
  ORDER BY c_int64 ASC NULLS FIRST, c_numeric_1 DESC NULLS LAST, c_bool_complex_expression ASC

),

Limit_1 AS (

  SELECT * 
  
  FROM OrderBy_1 AS in0
  
  LIMIT 45

)

SELECT * 

FROM Limit_1
