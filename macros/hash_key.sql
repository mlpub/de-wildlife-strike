{% macro hash_key(fields) -%}
ABS(FARM_FINGERPRINT(TO_JSON_STRING(STRUCT({{ fields | join(', ') }}))))
{%- endmacro %}