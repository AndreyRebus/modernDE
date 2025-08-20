select *
from {{ source('lol_raw', 'data_api_mining') }}
where source_nickname not like '%#RU1'
