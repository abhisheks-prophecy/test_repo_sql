{% macro qa_number_macro(input_number) %}
ST_PERIMETER(TO_GEOGRAPHY('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')) + ST_HAUSDORFFDISTANCE(ST_POINT(0, 0), ST_POINT(0, 1)) + EXTRACT(YEAR FROM TO_TIMESTAMP('2013-05-08T23:39:20.123-07:00')) + DATE_PART(QUARTER, '2013-05-08'::DATE) + abs(-10) + ceil(10.12) + floor(12.5656) + mod({{input_number}}, 2) + round(-975.975, 1) + SIGN(-1.35E-10) + truncate(4.23423) + truncate({{input_number}}, 2) + cbrt(8) + exp(2) + factorial(1) + pow(2, 3) + power(1, {{input_number}}) + sqrt(4) + square(2) + ln(10) + log(10, 10) + COS(0) + COS(PI() / 3) + COS(RADIANS(90)) + SIN(0) + SIN(PI() / 3) + SIN(RADIANS(90)) - HAVERSINE(40.7127, -74.0059, 34.05, -118.25) + DAYOFMONTH('2013-05-08T23:39:20.123-07:00'::TIMESTAMP)
{% endmacro %}

 {% macro qa_concat_macro(input_string) %}
concat(TRIM('❄-❄ABC-❄-', '❄-'), REPLACE({{input_string}}, 'bc'), RIGHT({{input_string}}, 3), CAST(HASH(SEQ8()) AS string), ASCII('A'), REPEAT('xy', 5), REVERSE('Hello, world!'), SUBSTR('testing 1 2 3', 9, 5), INSERT('abc', 1, 2, 'Z'), RTRIM('$125.00', '0.'), UUID_STRING(), sha1({{input_string}}), CAST(md5_binary({{input_string}}) AS string), LPAD(' hello ', 10, ' '), DECOMPRESS_STRING(TO_BINARY('0920536E6F77666C616B65', 'HEX'), 'SNAPPY'), LPAD('.  hi. ', 10, '$'), DAYNAME(TO_DATE('2015-05-01')), CAST(LAST_DAY(TO_DATE('2015-05-08T23:39:20.123-07:00')) AS string), CAST(DATEADD(YEAR, 2, TO_DATE('2013-05-08')) AS string), CAST(DATEDIFF(month, '2021-01-01'::DATE, '2021-02-28'::DATE) AS string), CAST(DATEDIFF(HOUR, '2013-05-08T23:39:20.123-07:00'::TIMESTAMP, DATEADD(YEAR, 2, ('2013-05-08T23:39:20.123-07:00')::TIMESTAMP)) AS string), CAST(TIMEDIFF(YEAR, '2017-01-01', '2019-01-01') AS string), CAST(TIME_SLICE('2019-02-28'::DATE, 4, 'MONTH', 'START') AS string), CAST(TRY_TO_TIME('12:30:00') AS string))
{% endmacro %}

 {% macro qa_boolean_macro(input_string) %}
startswith('sasd', 'te') or REGEXP_LIKE({{input_string}}, 'san.*') or RLIKE({{input_string}}, 'san.*', 'i') or CONTAINS({{input_string}}, 'te') or ({{input_string}} LIKE '%j%h%do%')
{% endmacro %}

 {% macro multi_macro_expressions(param_float=12, param_array=[1,2,3], param_dict=[1,2,3]) %}
concat({{param_float}} + {{param_array[0]}}, 'hello')
{% endmacro %}

 