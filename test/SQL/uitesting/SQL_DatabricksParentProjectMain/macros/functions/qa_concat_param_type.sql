{% macro qa_concat_param_type(input_string='hello', int_param=10) %}
concat({{input_string}}, {{int_param}})
{% endmacro %}

 {% macro multi_macro_expressions(param_float=12, param_array=[1, 2, 3], param_dict=[1, 2, 3]) %}
concat({{param_float}} + {{param_array[0]}}, 'hello')
{% endmacro %}

 {% macro round_function(n1, scale=2) %}
ROUND({{n1}}, {{scale}})
{% endmacro %}

 {% macro qa_macro_call_another_macro(final_param='random data') %}
concat({{ qa_concat_macro(final_param) }}, {{final_param}})
{% endmacro %}

 {% macro qa_concat_macro(input_string='hello') %}
concat(aes_decrypt(unhex('83F16B2AA704794132802D248E6BFD4E380078182D1544813898AC97E709B28A94'), '0000111122223333'), base64(aes_encrypt('Spark SQL', '1234567890abcdef', 'ECB', 'PKCS')), bin(13), btrim('    SparkSQL   '), char(65), chr(65), concat('{{input_string}}', 'SQL'), concat_ws(' ', 'Spark', 'SQL'), crc32('Spark'), current_catalog(), current_database(), current_date(), current_timestamp(), current_timezone(), current_user(), date_add('2016-07-30', 1), date_sub('2016-07-30', 1), date_format('2016-04-08', 'y'), date_from_unix_date(1), date_part('YEAR', TIMESTAMP'2019-08-12 01:00:00.123456'), date_part('MONTH', INTERVAL '2021-11' YEAR TO MONTH), date_part('MINUTE', INTERVAL '123 23:55:59.002001' DAY TO SECOND), date_trunc('HOUR', '2015-03-05T09:32:05.359'), date_trunc('DD', '2015-03-05T09:32:05.359'), datediff('2009-07-31', '2009-07-30'), decode(encode('abc', 'utf-8'), 'utf-8'), e(), elt(1, 'scala', 'java'), format_number(12332.123456, '##################.###'), format_string('Hello World %d %s', 100, 'days'), CAST(from_csv('1, 0.8', 'a INT, b DOUBLE') AS string), CAST(from_json('{"teacher": "Alice", "student": [{"name": "Bob", "rank": 1}, {"name": "Charlie", "rank": 2}]}', 'STRUCT<teacher: STRING, student: ARRAY<STRUCT<name: STRING, rank: INT>>>') AS string), CAST(from_unixtime(0, 'yyyy-MM-dd HH:mm:ss') AS string), CAST(from_utc_timestamp('2016-08-31', 'Asia/Seoul') AS string), CAST(get_json_object('{"a":"b"}', '$.a') AS string), hash('Spark', array(123), 2), hex(17), CAST(hour('2009-07-30 12:58:59') AS string), CAST(hypot(3, 4) AS string), CAST(ilike('Spark', '_Park') AS string))
{% endmacro %}

 {% macro qa_boolean_macro(input_string) %}
(1 != 2) or (true != NULL) or (NULL != NULL) or (1 < 2) or (2 <= 2) or (2 <=> 2) or ((2 % 1.8) == 1) or (to_date('2009-07-30 04:17:52') < to_date('2009-07-30 04:17:52')) or (add_months('2016-08-31', 1) < add_months('2017-08-31', 3)) or (true and false) or array_contains(array_distinct(array(1, 2, 3)), 2) or array_contains(array_except(array(1, 2, 3), array(1, 3, 5)), 2) or array_contains(array_intersect(array(1, 2, 3), array(1, 3, 5)), 10) or (array_join(array('hello', NULL, 'world'), ' ', ',') LIKE '%hello%') or (array_max(array(1, 20, NULL, 3)) > 10) or (array_min(array(1, 20, NULL, 3)) > 20) or array_contains(array_remove(array(1, 2, 3, NULL, 3), 3), 3) or array_contains(array_repeat(5, 2), 6) or array_contains(array_union(array(1, 2, 3), array(1, 3, 5)), 10) or arrays_overlap(array(1, 2, 3), array(3, 4, 5)) or (10 BETWEEN 2 and 20) or contains('Spark SQL', 'Spark') or endswith('Spark SQL', 'SQL') or (EXISTS (array(1, 2, 3), 
x -> x % 2 == 0)) or array_contains(filter(array(1, 2, 3), 
x -> x % 2 == 1), 5) or array_contains(flatten(array(array(1, 2), array(3, 4))), 10) or forall(array(1, 2, 3), 
x -> x % 2 == 0) or ilike('Spark', '_Park') or (1 IN (2, 3, 4)) or (isnan(CAST('NaN' AS double))) or isnotnull(1) or isnull(1) or array_contains(json_object_keys('{"key": "value"}'), 'key1') or like('{{input_string}}', '_park') or map_contains_key(map(1, 'a', 2, 'b'), 1) or map_contains_key(map_concat(map(1, 'a', 2, 'b'), map(3, 'c')), 4) or map_contains_key(map_filter(map(1, 0, 2, 2, 3, -1), 
(k, v) -> k > v), 3) or map_contains_key(map_from_arrays(array(1.0, 3.0), array('2', '4')), 2) or map_contains_key(map_from_entries(array(struct(1, 'a'), struct(2, 'b'))), 1) or array_contains(map_keys(map(1, 'a', 2, 'b')), 2) or array_contains(map_values(map(1, 'a', 2, 'b')), 'a') or map_contains_key(map_zip_with(map(1, 'a', 2, 'b'), map(1, 'x', 2, 'y'), 
(k, v1, v2) -> concat(v1, v2)), 1) or (named_struct('a', 1, 'b', 2) IN (named_struct('a', 1, 'b', 1), named_struct('a', 1, 'b', 3))) or (NOT true) or regexp('%SystemDrive%\\Users\\John', '%SystemDrive%\\\\Users.*') or array_contains(regexp_extract_all('100-200, 300-400', '(\\d+)-(\\d+)', 1), '100') or (rlike('%SystemDrive%\\Users\\John', '%SystemDrive%\\\\Users.*')) or array_contains(sequence(1, 5), 4) or array_contains(shuffle(array(1, 20, 3, 5)), 10) or array_contains(slice(array(1, 2, 3, 4), 2, 2), 4) or array_contains(sort_array(array('b', 'd', NULL, 'c', 'a'), true), 'b') or array_contains(split('oneAtwoBthreeC', '[ABC]'), 'one') or startswith('Spark SQL', 'Spark') or map_contains_key(str_to_map('a:1,b:2,c:3', ',', ':'), 'a') or array_contains(transform(array(1, 2, 3), 
x -> x + 1), 1) or map_contains_key(transform_keys(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), 
(k, v) -> k + 1), 1) or map_contains_key(transform_values(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), 
(k, v) -> v + 1), 2) or array_contains(xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/b/text()'), 'a') or xpath_boolean('<a><b>1</b></a>', 'a/b') or array_contains(zip_with(array(1, 2), array(3, 4), 
(x, y) -> x + y), 1) or h3_boundaryasgeojson(599686042433355775) IS NOT NULL or hex(h3_boundaryaswkb(599686042433355775)) IS NOT NULL or h3_centerasgeojson('8009fffffffffff') IS NOT NULL or ARRAY_SIZE(h3_compact(h3_kring(599686042433355775, 2))) IS NOT NULL or h3_distance('85283447fffffff', '8528340ffffffff') IS NOT NULL or h3_hexring('85283473fffffff', 1) IS NOT NULL
{% endmacro %}

 {% macro qa_number_macro(input_number=10) %}
(2 % 1.8) + (MOD(2, 1.8)) + ({{input_number}} & 5) + (2 * 3) + ({{input_number}} + 10) - (100 + 45) + (3 / 2) + (3 ^ 5) + abs(-1) + acos(1) + acosh(1) + array_position(array(3, 2, 1), 1) + array_size(array('b', 'd', 'c', 'a')) + ascii(2) + asin(0) + asinh(0) + atan(0) + atan2(0, 0) + atanh(0) + bit_count(0) + bit_get(11, 0) + bit_length('Spark SQL') + bround(25, -1) + cardinality(array('b', 'd', 'c', 'a')) + cardinality(map('a', 1, 'b', 2)) + CAST('10' AS int) + cbrt(27.0) + ceil(3.1411, 3) + ceiling(3.1411, 3) + char_length('Spark SQL ') + coalesce(NULL, 1, NULL) + conv('100', 2, 10) + cos(0) + cosh(0) + cot(1) + csc(1) + day('2009-07-30') + dayofmonth('2009-07-30') + dayofweek('2009-07-30') + dayofyear('2016-04-09') + degrees(3.141592653589793) + element_at(array(1, 2, 3), 2) + exp(0) + expm1(0) + EXTRACT(SECONDS FROM TIMESTAMP'2019-10-01 00:00:01.000001') + EXTRACT(MINUTE FROM INTERVAL '123 23:55:59.002001' DAY TO SECOND) + factorial(2) + find_in_set('ab', 'abc,b,ab,c,def') + floor(-0.1) + getbit(11, 0) + greatest(10, 9, 2, 4, 3) + instr('SparkSQL', 'SQL') + json_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]') + least(10, 9, 2, 4, 3) + length('Spark SQL ') + levenshtein('kitten', 'sitting') + ln(10) + locate('bar', 'foobarbar') + log(10, 100) + log10(10) + log1p(0) + log2(2) + minute('2009-07-30 12:58:59') + (2 % 1.8) + month('2016-07-30') + months_between('1997-02-28 10:30:00', '1996-10-30', false) + nanvl(CAST('NaN' AS double), 123) + negative(100) + nvl2(NULL, 2, 1) + octet_length('Spark SQL') + pi() + pmod(10, 3) + position('bar', 'foobarbar') + positive(1) + pow(2, 3) + power(2, 3) + quarter('2016-08-31') + radians(180) + rand() + randn() + random() + rint(12.3456) + round(2.5, 0) + sec(0) + second('2009-07-30 12:58:59') + shiftleft(2, 1) + shiftright(4, 1) + shiftrightunsigned(4, 1) + sign(40) + signum(40) + sin(0) + sinh(0) + size(array('b', 'd', 'c', 'a')) + sqrt(4) + tan(0) + tanh(0) + to_number('454.00', '000.00') + try_add(1, 2) + try_divide(2L, 2L) + try_element_at(array(1, 2, 3), 2) + try_multiply(2, 3) + try_subtract(2, 1) + weekday('2009-07-30') + weekofyear('2008-02-20') + (CASE
  WHEN 1 > 0
    THEN 1
  WHEN 2 > 0
    THEN 2.0
  ELSE 1.2
END) + width_bucket(5.3, 0.2, 10.6, 5) + xpath_double('<a><b>1</b><b>2</b></a>', 'sum(a/b)') + xpath_int('<a><b>1</b><b>2</b></a>', 'sum(a/b)') + xpath_long('<a><b>1</b><b>2</b></a>', 'sum(a/b)') + xpath_number('<a><b>1</b><b>2</b></a>', 'sum(a/b)') + xpath_short('<a><b>1</b><b>2</b></a>', 'sum(a/b)') + (~ 0)
{% endmacro %}

 