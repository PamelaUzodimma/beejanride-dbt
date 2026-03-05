{% macro is_fraud_indicator(surge_col, threshold=10) %}
    case when {{ surge_col }} > {{ threshold }} then true else false end
{% endmacro %}
