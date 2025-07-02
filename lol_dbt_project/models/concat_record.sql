{{ config(materialized='table') }}

{% set tables = [
    '2pilka_record',
    'breakthesilence_record',
    'gruntq_record',
    'monty_record',
    'prooaknor_record',
    'shazam_record'
] %}

{% for table in tables %}
    {% if not loop.first %}
        union all
    {% endif %}
    select *
    from {{ ref(table) }}
{% endfor %}