select *
from {{ source('lol_raw', 'data_api_mining') }}
where "info.gameDuration" <= 0
   or "info.gameDuration" > 720000
