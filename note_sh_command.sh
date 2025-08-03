docker compose build dagster      # 1 раз соберёт образ
docker compose up -d dagster      # запустит Dagit
docker compose logs -f dagster    # смотрим, что UI поднялся на 0.0.0.0:3000

docker build --no-cache -t load .        
docker run --rm --env-file .env load
