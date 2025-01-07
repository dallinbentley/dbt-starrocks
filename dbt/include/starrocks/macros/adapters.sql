{% macro starrocks__create_materialized_view(relation, sql) %}
  create materialized view {{ relation }}
  {% if config.get('refresh_type', none) %}
    REFRESH {{ config.get('refresh_type') }}
  {% endif %}
  {% if config.get('partition_by', none) %}
    PARTITION BY ({{ config.get('partition_by') | join(', ') }})
  {% endif %}
  {% if config.get('distributed_by', none) %}
    DISTRIBUTED BY HASH({{ config.get('distributed_by') | join(', ') }})
  {% endif %}
  {% if config.get('buckets', none) %}
    BUCKETS {{ config.get('buckets') }}
  {% endif %}
  as (
    {{ sql }}
  );
{% endmacro %}

{% macro starrocks__get_create_index_sql(relation, index_dict) -%}
  create index {{ index_dict['name'] }} on {{ relation }}
  (
    {% for column in index_dict['columns'] %}
      {{ column }}{% if not loop.last %}, {% endif %}
    {% endfor %}
  )
  {% if index_dict.get('unique', false) %}unique{% endif %}
  {% if index_dict.get('type', none) %}
    using {{ index_dict['type'] }}
  {% endif %}
{%- endmacro %} 