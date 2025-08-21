cd /home/modernDE
docker run --rm --env-file .env load 
docker run --rm -it \
  --env-file .env \
  -v "$(pwd)/lol_dbt_project:/workspace" \
  dbt dbt run --project-dir /workspace

docker compose up -d --force-recreate --no-deps bot