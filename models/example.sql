{{ config(
    materialized='table',
    table_type='DUPLICATE',
    distributed_by=['id']
) }}

select * from users 