/*{% test min_char_count(model, column_name, value) %}
    select *
    from {{ model }}
    where 
        length({{ column_name }}) < {{ value }}
{% endtest %}*/