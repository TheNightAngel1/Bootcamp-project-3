/*{% test min_word_count(model, column_name, value) %}
    select *
    from {{ model }}
    where length({{ column_name }}) - length(replace({{ column_name }}, ' ', '')) + 1 < {{ value }}
{% endtest %}*/