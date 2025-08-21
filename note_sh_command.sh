docker compose build dagster      # 1 раз соберёт образ
docker compose up -d dagster      # запустит Dagit
docker compose logs -f dagster    # смотрим, что UI поднялся на 0.0.0.0:3000

docker build --no-cache -t load .
docker run --rm \
  --network modernde_trino_network \
  --env-file .env \
  load

docker build --no-cache --pull -f Dockerfile.dbt -t dbt .
docker run --rm -it \
  --env-file .env \
  -v "$(pwd)/lol_dbt_project:/workspace" \
  dbt dbt run --project-dir /workspace

docker build --no-cache --pull -f Dockerfile.splashes -t lol-splashes .
docker run --rm \
  -v /home/modernDE/bot/data/splashes:/app/data/splashes \
  lol-splashes


docker build -f Dockerfile.deps -t bot-deps .

docker run --rm \
  --name bot \
  --env-file .env \
  -w /app \
  -v "$PWD/bot:/app/bot" \
  -v "$PWD/data:/app/data" \
  -v "$PWD/bot/data/splashes:/app/data/splashes:ro" \
  bot-deps bash -lc 'python -m bot.prefetch && exec python -m bot.bot'
