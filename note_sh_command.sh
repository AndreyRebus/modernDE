docker compose build dagster      # 1 раз соберёт образ
docker compose up -d dagster      # запустит Dagit
docker compose logs -f dagster    # смотрим, что UI поднялся на 0.0.0.0:3000

docker build --no-cache -t load .      
docker run --rm --env-file .env load 

docker build --no-cache --pull -f Dockerfile.dbt -t dbt .
docker run --rm -it \
  --env-file .env \
  -v "$(pwd)/lol_dbt_project:/workspace" \
  dbt dbt run --project-dir /workspace

docker build --no-cache --pull -f Dockerfile.splashes -t lol-splashes .
docker run --rm -v /home/modernDE/bot/data/splashes:/data/splashes -w /app lol-splashes

#-------------------------------#------------------#
docker build -f Dockerfile.mybot -t mybot .
# Запуск + сборка образа (если нужно)
docker compose up -d --build mybot

# Остановка
docker compose stop mybot

# Перезапуск
docker compose restart mybot
#logs
docker compose logs -f mybot
