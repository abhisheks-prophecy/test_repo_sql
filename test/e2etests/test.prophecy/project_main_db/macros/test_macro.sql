{% macro test_macro(c_int=10, c_long=100, c_decimal=4.4545) %}
((((((((((((((((((((((((((((greatest({{c_int}}, 9, 2) + floor({{c_decimal}})) + ((degrees(3.141592653589793D) * exp(2)) * expm1(0))) + factorial(5)) + format_number(12332.123456D, 4)) - instr('SparkSQL', 'SQL')) - length('Spark SQL ')) - levenshtein('kitten', 'sitting')) + ((call_func('log', 10.0D, 100) * log10(10)) * log2(2))) + locate('bar', 'foobarbar', 5)) - months_between('1997-02-28 10:30:00', '1996-10-30')) + nanvl(CAST('NaN' AS DOUBLE), 123)) + rand()) + (10 % 3)) - round(CAST('2.5' AS FLOAT), 0)) + (sin(0) * sinh(0))) + sqrt(4)) + abs(1.23D)) + acos(1)) - ascii('2')) - asin(0)) + bin(13)) + CAST('10' AS INT)) + cbrt(CAST('27.0' AS FLOAT))) + ceil(-2.1D)) - coalesce(NULL, 1, NULL)) + conv('100', 2, 10)) + year('2016-07-30')) + least({{c_decimal}}, {{c_int}}, {{c_long}}))
{% endmacro %}

 