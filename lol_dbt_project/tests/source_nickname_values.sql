select *
from {{ source('lol_raw', 'data_api_mining') }}
where source_nickname not in (
    'Monty Gard#RU1',
    'Breaksthesilence#RU1',
    '2pilka#RU1',
    'Prooaknor#RU1',
    'Шaзам#RU1',
    'Gruntq#RU1'
)
