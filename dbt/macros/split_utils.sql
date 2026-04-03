{% macro first_element(column, delimiter) %}
    SPLIT({{ column }}, {{ delimiter }})[SAFE_OFFSET(0)]
{% endmacro %}