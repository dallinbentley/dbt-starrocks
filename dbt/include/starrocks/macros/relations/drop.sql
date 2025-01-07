{% macro drop_relation_if_exists(relation, sql=none) -%}
  {% if relation is not none %}
      {{ adapter.drop_relation(relation) }}
  {% endif %}
{%- endmacro %} 