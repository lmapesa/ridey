{% macro safe_concat(field_list) %}

  concat({% for f in field_list %}
    ifnull(safe_cast({{ f }} as string), '')
    {% if not loop.last %}, {% endif %}
  {% endfor %})
{% endmacro %}