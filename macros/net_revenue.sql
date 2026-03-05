{% macro calculate_net_revenue(gross_revenue_col, fee_col) %}
    ({{ gross_revenue_col }} - {{ fee_col }})
{% endmacro %}
